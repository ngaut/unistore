// Copyright 2019-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package raftstore

import (
	"github.com/ngaut/unistore/raftengine"
	"github.com/pingcap/log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/unistore/metrics"
)

// peerState contains the peer states that needs to run raft command and apply command.
// It binds to a worker to make sure the commands are always executed on a same goroutine.
type peerState struct {
	closed uint32
	peer   *peerFsm
	apply  *applier
}

type applyBatch struct {
	msgs      []Msg
	peers     map[uint64]*peerState
	proposals []*regionProposal
}

func newApplyBatch() *applyBatch {
	return &applyBatch{peers: map[uint64]*peerState{}}
}

func (b *applyBatch) iterCallbacks(f func(cb *Callback)) {
	for _, rp := range b.proposals {
		for _, p := range rp.Props {
			if p.cb != nil {
				f(p.cb)
			}
		}
	}
}

// raftWorker is responsible for run raft commands and apply raft logs.
type raftWorker struct {
	pr *router

	raftCh        chan Msg
	raftCtx       *RaftContext
	raftStartTime time.Time

	applyCh    chan *applyBatch
	applyResCh chan Msg
	applyCtx   *applyContext

	msgCnt            uint64
	movePeerCandidate uint64
	closeCh           <-chan struct{}

	topWriteDurations []time.Duration
	writeDurTotal     time.Duration
}

func newRaftWorker(ctx *GlobalContext, ch chan Msg, pm *router) *raftWorker {
	raftCtx := &RaftContext{
		GlobalContext: ctx,
		applyMsgs:     new(applyMsgs),
		raftWB:        raftengine.NewWriteBatch(),
		localStats:    new(storeStats),
	}
	applyResCh := make(chan Msg, cap(ch))
	return &raftWorker{
		raftCh:     ch,
		applyResCh: applyResCh,
		raftCtx:    raftCtx,
		pr:         pm,
		applyCh:    make(chan *applyBatch, 1),
		applyCtx:   newApplyContext("", ctx.regionTaskSender, ctx.engine, applyResCh, ctx.cfg),
	}
}

// run runs raft commands.
// On each loop, raft commands are batched by channel buffer.
// After commands are handled, we collect apply messages by peers, make a applyBatch, send it to apply channel.
func (rw *raftWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	timeTicker := time.NewTicker(rw.raftCtx.cfg.RaftBaseTickInterval)
	var msgs []Msg
	for {
		for i := range msgs {
			msgs[i] = Msg{}
		}
		msgs = msgs[:0]
		select {
		case <-closeCh:
			rw.applyCh <- nil
			return
		case msg := <-rw.raftCh:
			msgs = append(msgs, msg)
		case msg := <-rw.applyResCh:
			msgs = append(msgs, msg)
		case <-timeTicker.C:
			rw.pr.peers.Range(func(key, value interface{}) bool {
				msgs = append(msgs, NewPeerMsg(MsgTypeTick, key.(uint64), nil))
				return true
			})
		}
		pending := len(rw.raftCh)
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-rw.raftCh)
		}
		resLen := len(rw.applyResCh)
		for i := 0; i < resLen; i++ {
			msgs = append(msgs, <-rw.applyResCh)
		}
		metrics.RaftBatchSize.Observe(float64(len(msgs)))
		atomic.AddUint64(&rw.msgCnt, uint64(len(msgs)))
		peerStateMap := make(map[uint64]*peerState)
		rw.raftCtx.pendingCount = 0
		rw.raftCtx.hasReady = false
		rw.raftStartTime = time.Now()
		batch := newApplyBatch()
		for _, msg := range msgs {
			peerState := rw.getPeerState(peerStateMap, msg.RegionID)
			h := newRaftMsgHandler(peerState.peer, rw.raftCtx)
			h.HandleMsgs(msg)
		}
		var movePeer uint64
		for id, peerState := range peerStateMap {
			movePeer = id
			h := newRaftMsgHandler(peerState.peer, rw.raftCtx)
			batch.proposals = h.HandleRaftReadyAppend(batch.proposals)
		}
		// Pick one peer as the candidate to be moved to other workers.
		atomic.StoreUint64(&rw.movePeerCandidate, movePeer)
		if rw.raftCtx.hasReady {
			rw.handleRaftReady(peerStateMap, batch)
		}
		rw.raftCtx.flushLocalStats()
		doneRaftTime := time.Now()
		batch.iterCallbacks(func(cb *Callback) {
			cb.raftBeginTime = rw.raftStartTime
			cb.raftDoneTime = doneRaftTime
		})
		applyMsgs := rw.raftCtx.applyMsgs
		batch.msgs = append(batch.msgs, applyMsgs.msgs...)
		for i := range applyMsgs.msgs {
			applyMsgs.msgs[i] = Msg{}
		}
		applyMsgs.msgs = applyMsgs.msgs[:0]
		rw.applyCh <- batch
	}
}

func (rw *raftWorker) getPeerState(peersMap map[uint64]*peerState, regionID uint64) *peerState {
	peer, ok := peersMap[regionID]
	if !ok {
		peer = rw.pr.get(regionID)
		peersMap[regionID] = peer
	}
	return peer
}

func (rw *raftWorker) handleRaftReady(peers map[uint64]*peerState, batch *applyBatch) {
	for _, proposal := range batch.proposals {
		msg := Msg{Type: MsgTypeApplyProposal, Data: proposal}
		rw.raftCtx.applyMsgs.appendMsg(proposal.RegionId, msg)
	}
	var dur time.Duration
	dur = rw.writeRaftWriteBatch()
	readyRes := rw.raftCtx.ReadyRes
	rw.raftCtx.ReadyRes = nil
	if len(readyRes) > 0 {
		for _, pair := range readyRes {
			h := newRaftMsgHandler(peers[pair.IC.Region.Id].peer, rw.raftCtx)
			h.HandleRaftReady(&pair.Ready, pair.IC)
		}
	}
	if !rw.raftCtx.isBusy {
		electionTimeout := rw.raftCtx.cfg.RaftBaseTickInterval * time.Duration(rw.raftCtx.cfg.RaftElectionTimeoutTicks)
		if dur > electionTimeout {
			rw.raftCtx.isBusy = true
		}
	}
}

func (rw *raftWorker) appendWriteDuration(dur time.Duration) {
	rw.writeDurTotal += dur
	if dur > 10*time.Millisecond {
		rw.topWriteDurations = append(rw.topWriteDurations, dur)
	}
	if rw.writeDurTotal > 10*time.Second {
		sort.Slice(rw.topWriteDurations, func(i, j int) bool {
			return rw.topWriteDurations[i] > rw.topWriteDurations[j]
		})
		if len(rw.topWriteDurations) > 10 {
			rw.topWriteDurations = rw.topWriteDurations[:10]
		}
		log.S().Infof("raft store write duration %v top:%v", rw.writeDurTotal, rw.topWriteDurations)
		rw.topWriteDurations = rw.topWriteDurations[:0]
		rw.writeDurTotal = 0
	}
}

func (rw *raftWorker) writeRaftWriteBatch() time.Duration {
	raftWB := rw.raftCtx.raftWB
	var dur time.Duration
	if !raftWB.IsEmpty() {
		begin := time.Now()
		err := rw.raftCtx.engine.raft.Write(raftWB)
		if err != nil {
			panic(err)
		}
		raftWB.Reset()
		dur = time.Since(begin)
		rw.appendWriteDuration(dur)
	}
	return dur
}

type applyWorker struct {
	r   *router
	ch  chan *applyBatch
	ctx *applyContext

	total time.Duration
	top   []time.Duration
}

func newApplyWorker(r *router, ch chan *applyBatch, ctx *applyContext) *applyWorker {
	return &applyWorker{
		r:   r,
		ch:  ch,
		ctx: ctx,
	}
}

// run runs apply tasks, since it is already batched by raftCh, we don't need to batch it here.
func (aw *applyWorker) run(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		batch := <-aw.ch
		if batch == nil {
			return
		}
		begin := time.Now()
		batch.iterCallbacks(func(cb *Callback) {
			cb.applyBeginTime = begin
		})
		for _, peer := range batch.peers {
			peer.apply.redoIndex = peer.apply.applyState.appliedIndex + 1
		}
		for _, msg := range batch.msgs {
			ps := batch.peers[msg.RegionID]
			if ps == nil {
				ps = aw.r.get(msg.RegionID)
				batch.peers[msg.RegionID] = ps
			}
			ps.apply.handleMsg(aw.ctx, msg)
		}
		aw.appendDuration(time.Since(begin))
	}
}

func (aw *applyWorker) appendDuration(dur time.Duration) {
	aw.total += dur
	if dur > 5*time.Millisecond {
		aw.top = append(aw.top, dur)
	}
	if aw.total > time.Second*10 {
		sort.Slice(aw.top, func(i, j int) bool {
			return aw.top[i] > aw.top[j]
		})
		if len(aw.top) > 10 {
			aw.top = aw.top[:10]
		}
		log.S().Infof("apply duration %v top:%v", aw.total, aw.top)
		aw.total = 0
		aw.top = aw.top[:0]
	}
}

// storeWorker runs store commands.
type storeWorker struct {
	store *storeMsgHandler
}

func newStoreWorker(ctx *GlobalContext, r *router) *storeWorker {
	storeCtx := &StoreContext{GlobalContext: ctx, applyingSnapCount: new(uint64)}
	return &storeWorker{
		store: newStoreFsmDelegate(r.storeFsm, storeCtx),
	}
}

func (sw *storeWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	timeTicker := time.NewTicker(sw.store.ctx.cfg.RaftBaseTickInterval)
	storeTicker := sw.store.ticker
	for {
		var msg Msg
		select {
		case <-closeCh:
			return
		case <-timeTicker.C:
			storeTicker.tickClock()
			for i := range storeTicker.schedules {
				if storeTicker.isOnStoreTick(StoreTick(i)) {
					sw.store.handleMsg(NewMsg(MsgTypeStoreTick, StoreTick(i)))
				}
			}
		case msg = <-sw.store.receiver:
		}
		sw.store.handleMsg(msg)
	}
}
