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
}

func newRaftWorker(ctx *GlobalContext, ch chan Msg, pm *router) *raftWorker {
	raftCtx := &RaftContext{
		GlobalContext: ctx,
		applyMsgs:     new(applyMsgs),
		queuedSnaps:   make(map[uint64]struct{}),
		kvWB:          new(WriteBatch),
		raftWB:        new(WriteBatch),
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
		batch := &applyBatch{
			peers: peerStateMap,
		}
		for _, msg := range msgs {
			peerState := rw.getPeerState(peerStateMap, msg.RegionID)
			newRaftMsgHandler(peerState.peer, rw.raftCtx).HandleMsgs(msg)
		}
		var movePeer uint64
		for id, peerState := range peerStateMap {
			movePeer = id
			batch.proposals = newRaftMsgHandler(peerState.peer, rw.raftCtx).HandleRaftReadyAppend(batch.proposals)
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
		rw.removeQueuedSnapshots()
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
	kvWB := rw.raftCtx.kvWB
	if len(kvWB.entries) > 0 {
		err := kvWB.WriteToKV(rw.raftCtx.engine.kv)
		if err != nil {
			panic(err)
		}
		kvWB.Reset()
	}
	raftWB := rw.raftCtx.raftWB
	if len(raftWB.entries) > 0 {
		err := raftWB.WriteToRaft(rw.raftCtx.engine.raft)
		if err != nil {
			panic(err)
		}
		raftWB.Reset()
	}
	readyRes := rw.raftCtx.ReadyRes
	rw.raftCtx.ReadyRes = nil
	if len(readyRes) > 0 {
		for _, pair := range readyRes {
			regionID := pair.IC.RegionID
			newRaftMsgHandler(peers[regionID].peer, rw.raftCtx).PostRaftReadyPersistent(&pair.Ready, pair.IC)
		}
	}
	dur := time.Since(rw.raftStartTime)
	if !rw.raftCtx.isBusy {
		electionTimeout := rw.raftCtx.cfg.RaftBaseTickInterval * time.Duration(rw.raftCtx.cfg.RaftElectionTimeoutTicks)
		if dur > electionTimeout {
			rw.raftCtx.isBusy = true
		}
	}
}

func (rw *raftWorker) removeQueuedSnapshots() {
	if len(rw.raftCtx.queuedSnaps) > 0 {
		rw.raftCtx.storeMetaLock.Lock()
		meta := rw.raftCtx.storeMeta
		retained := meta.pendingSnapshotRegions[:0]
		for _, region := range meta.pendingSnapshotRegions {
			if _, ok := rw.raftCtx.queuedSnaps[region.Id]; !ok {
				retained = append(retained, region)
			}
		}
		meta.pendingSnapshotRegions = retained
		rw.raftCtx.storeMetaLock.Unlock()
		rw.raftCtx.queuedSnaps = map[uint64]struct{}{}
	}
}

type applyWorker struct {
	r   *router
	ch  chan *applyBatch
	ctx *applyContext
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
			ps.apply.handleTask(aw.ctx, msg)
		}
		aw.ctx.flush()
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
