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
	"encoding/binary"
	"fmt"
	"github.com/dgryski/go-farm"
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
	peers map[uint64]*peerApplyBatch
}

type peerApplyBatch struct {
	apply     *applier
	applyMsgs []Msg
}

func newApplyBatch() *applyBatch {
	return &applyBatch{peers: map[uint64]*peerApplyBatch{}}
}

func (ab *applyBatch) group(cnt int) [][]*peerApplyBatch {
	groups := make([][]*peerApplyBatch, cnt)
	for regionID, peerBatch := range ab.peers {
		idx := hashRegionID(regionID) % uint64(cnt)
		groups[idx] = append(groups[idx], peerBatch)
	}
	return groups
}

func hashRegionID(regionID uint64) uint64 {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, regionID)
	return farm.Fingerprint64(b)
}

// raftWorker is responsible for run raft commands and apply raft logs.
type raftWorker struct {
	pr *router

	msgs    []Msg
	ticker  *time.Ticker
	raftCh  chan Msg
	raftCtx *RaftContext

	applyChs   []chan []*peerApplyBatch
	applyCtxes []*applyContext
	applyResCh chan Msg

	movePeerCandidate uint64
	closeCh           <-chan struct{}

	handleMsgDc   *durationCollector
	readyAppendDc *durationCollector
	writeDc       *durationCollector
	handleReadyDc *durationCollector
}

func newRaftWorker(ctx *GlobalContext, ch chan Msg, pm *router, applyWorkerCnt int) *raftWorker {
	raftCtx := &RaftContext{
		GlobalContext: ctx,
		applyMsgs:     new(applyMsgs),
		raftWB:        raftengine.NewWriteBatch(),
		localStats:    new(storeStats),
	}
	applyResCh := make(chan Msg, cap(ch))
	applyChs := make([]chan []*peerApplyBatch, applyWorkerCnt)
	applyCtxes := make([]*applyContext, applyWorkerCnt)
	for i := 0; i < applyWorkerCnt; i++ {
		applyChs[i] = make(chan []*peerApplyBatch, 1)
		applyCtxes[i] = newApplyContext("", ctx.regionTaskSender, ctx.engine, applyResCh, ctx.cfg)
	}
	return &raftWorker{
		raftCh:        ch,
		applyResCh:    applyResCh,
		raftCtx:       raftCtx,
		pr:            pm,
		applyChs:      applyChs,
		applyCtxes:    applyCtxes,
		handleMsgDc:   newDurationCollector("raft_handle_msg"),
		readyAppendDc: newDurationCollector("raft_ready_append"),
		writeDc:       newDurationCollector("raft_write"),
		handleReadyDc: newDurationCollector("raft_handle_ready"),
	}
}

// run runs raft commands.
// On each loop, raft commands are batched by channel buffer.
// After commands are handled, we collect apply messages by peers, make a applyBatch, send it to apply channel.
func (rw *raftWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	rw.ticker = time.NewTicker(rw.raftCtx.cfg.RaftBaseTickInterval)
	for {
		if quit := rw.receiveMsgs(closeCh); quit {
			return
		}
		peerStateMap := rw.handleMsgs()
		rw.handleRaftReadyAppend(peerStateMap)
		rw.writeRaftWriteBatch()
		rw.handleRaftReady(peerStateMap)
		rw.raftCtx.flushLocalStats()
		rw.scheduleApply()
	}
}

func (rw *raftWorker) receiveMsgs(closeCh <-chan struct{}) (quit bool) {
	for i := range rw.msgs {
		rw.msgs[i] = Msg{}
	}
	rw.msgs = rw.msgs[:0]
	select {
	case <-closeCh:
		for _, applyCh := range rw.applyChs {
			applyCh <- nil
		}
		return true
	case msg := <-rw.raftCh:
		rw.msgs = append(rw.msgs, msg)
	case msg := <-rw.applyResCh:
		rw.msgs = append(rw.msgs, msg)
	case <-rw.ticker.C:
		rw.pr.peers.Range(func(key, value interface{}) bool {
			rw.msgs = append(rw.msgs, NewPeerMsg(MsgTypeTick, key.(uint64), nil))
			return true
		})
	}
	pending := len(rw.raftCh)
	for i := 0; i < pending; i++ {
		rw.msgs = append(rw.msgs, <-rw.raftCh)
	}
	resLen := len(rw.applyResCh)
	for i := 0; i < resLen; i++ {
		rw.msgs = append(rw.msgs, <-rw.applyResCh)
	}
	metrics.RaftBatchSize.Observe(float64(len(rw.msgs)))
	return false
}

func (rw *raftWorker) handleMsgs() (peerStateMap map[uint64]*peerState) {
	begin := time.Now()
	rw.raftCtx.pendingCount = 0
	peerStateMap = make(map[uint64]*peerState)
	for _, msg := range rw.msgs {
		peerState := rw.getPeerState(peerStateMap, msg.RegionID)
		h := newRaftMsgHandler(peerState.peer, rw.raftCtx)
		h.HandleMsgs(msg)
	}
	rw.handleMsgDc.collect(time.Since(begin))
	return peerStateMap
}

func (rw *raftWorker) handleRaftReadyAppend(peerStateMap map[uint64]*peerState) {
	begin := time.Now()
	var movePeer uint64
	var proposals []*regionProposal
	for id, peerState := range peerStateMap {
		movePeer = id
		h := newRaftMsgHandler(peerState.peer, rw.raftCtx)
		proposals = h.HandleRaftReadyAppend(proposals)
	}
	for _, proposal := range proposals {
		msg := Msg{Type: MsgTypeApplyProposal, Data: proposal}
		rw.raftCtx.applyMsgs.appendMsg(proposal.RegionId, msg)
	}
	// Pick one peer as the candidate to be moved to other workers.
	atomic.StoreUint64(&rw.movePeerCandidate, movePeer)
	rw.readyAppendDc.collect(time.Since(begin))
}

func (rw *raftWorker) getPeerState(peersMap map[uint64]*peerState, regionID uint64) *peerState {
	peer, ok := peersMap[regionID]
	if !ok {
		peer = rw.pr.get(regionID)
		peersMap[regionID] = peer
	}
	return peer
}

func (rw *raftWorker) handleRaftReady(peers map[uint64]*peerState) {
	readyRes := rw.raftCtx.ReadyRes
	if len(readyRes) > 0 {
		rw.raftCtx.ReadyRes = nil
		begin := time.Now()
		for _, pair := range readyRes {
			h := newRaftMsgHandler(peers[pair.IC.Region.Id].peer, rw.raftCtx)
			h.HandleRaftReady(&pair.Ready, pair.IC)
		}
		rw.handleReadyDc.collect(time.Since(begin))
	}
}

func (rw *raftWorker) writeRaftWriteBatch() {
	raftWB := rw.raftCtx.raftWB
	if !raftWB.IsEmpty() {
		begin := time.Now()
		err := rw.raftCtx.engine.raft.Write(raftWB)
		if err != nil {
			panic(err)
		}
		raftWB.Reset()
		rw.writeDc.collect(time.Since(begin))
	}
}

func (rw *raftWorker) scheduleApply() {
	applyMsgs := rw.raftCtx.applyMsgs
	batch := newApplyBatch()
	for i, msg := range applyMsgs.msgs {
		peerBatch := batch.peers[msg.RegionID]
		if peerBatch == nil {
			peerState := rw.pr.get(msg.RegionID)
			if peerState == nil {
				log.S().Warnf("region %d peer state is nil", msg.RegionID)
				continue
			}
			peerBatch = &peerApplyBatch{
				apply: peerState.apply,
			}
			batch.peers[msg.RegionID] = peerBatch
		}
		peerBatch.applyMsgs = append(peerBatch.applyMsgs, msg)
		applyMsgs.msgs[i] = Msg{}
	}
	applyMsgs.msgs = applyMsgs.msgs[:0]
	groups := batch.group(len(rw.applyChs))
	for i, group := range groups {
		if len(group) > 0 {
			rw.applyChs[i] <- group
		}
	}
}

type applyWorker struct {
	idx int
	r   *router
	ch  chan []*peerApplyBatch
	ctx *applyContext
	dc  *durationCollector
}

func newApplyWorker(r *router, idx int, ch chan []*peerApplyBatch, ctx *applyContext) *applyWorker {
	return &applyWorker{
		idx: idx,
		r:   r,
		ch:  ch,
		ctx: ctx,
		dc:  newDurationCollector(fmt.Sprintf("apply%d", idx)),
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
		for _, peerBatch := range batch {
			for _, msg := range peerBatch.applyMsgs {
				peerBatch.apply.handleMsg(aw.ctx, msg)
			}
		}
		aw.dc.collect(time.Since(begin))
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

type durationCollector struct {
	name      string
	top       []time.Duration
	total     time.Duration
	cnt       int
	lastPrint time.Time
}

const printInterval = time.Second * 10

func newDurationCollector(name string) *durationCollector {
	return &durationCollector{name: name, lastPrint: time.Now()}
}

func (dc *durationCollector) collect(dur time.Duration) {
	dc.total += dur
	dc.cnt++
	if dur > 5*time.Millisecond {
		dc.top = append(dc.top, dur)
	}
	if dc.total > printInterval {
		sort.Slice(dc.top, func(i, j int) bool {
			return dc.top[i] > dc.top[j]
		})
		if len(dc.top) > 10 {
			dc.top = dc.top[:10]
		}
		log.S().Infof("%s duration:%v/%v count:%d top:%v", dc.name, dc.total, time.Since(dc.lastPrint), dc.cnt, dc.top)
		dc.reset()
	}
}

func (dc *durationCollector) reset() {
	dc.total = 0
	dc.cnt = 0
	dc.top = dc.top[:0]
	dc.lastPrint = time.Now()
}
