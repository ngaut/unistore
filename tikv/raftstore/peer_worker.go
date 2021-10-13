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

type peerInbox struct {
	peer *peerState
	msgs []Msg
}

func (pi *peerInbox) reset() {
	for i := range pi.msgs {
		pi.msgs[i] = Msg{}
	}
	pi.msgs = pi.msgs[:0]
}

func (pi *peerInbox) append(msg Msg) {
	pi.msgs = append(pi.msgs, msg)
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

	inboxes map[uint64]*peerInbox
	ticker  *time.Ticker
	raftCh  chan Msg
	raftCtx *RaftContext

	applyChs   []chan *peerApplyBatch
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
	applyChs := make([]chan *peerApplyBatch, applyWorkerCnt)
	applyCtxes := make([]*applyContext, applyWorkerCnt)
	for i := 0; i < applyWorkerCnt; i++ {
		applyChs[i] = make(chan *peerApplyBatch, 256)
		applyCtxes[i] = newApplyContext("", ctx.regionTaskSender, ctx.engine, applyResCh, ctx.cfg)
	}
	return &raftWorker{
		raftCh:        ch,
		applyResCh:    applyResCh,
		inboxes:       map[uint64]*peerInbox{},
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
		loopStartTime := time.Now()
		if quit := rw.receiveMsgs(closeCh); quit {
			return
		}
		processStartTime := time.Now()
		var readyRes []*ReadyICPair
		for _, inbox := range rw.inboxes {
			rd := rw.processInBox(inbox)
			if rd != nil {
				readyRes = append(readyRes, rd)
			}
		}
		metrics.ProcessRegionDurationHistogram.Observe(time.Since(processStartTime).Seconds())
		rw.persistState()
		rw.postPersistState(readyRes)
		rw.raftCtx.flushLocalStats()
		metrics.RaftWorkerLoopDurationHistogram.Observe(time.Since(loopStartTime).Seconds())
	}
}

func (rw *raftWorker) receiveMsgs(closeCh <-chan struct{}) (quit bool) {
	begin := time.Now()
	for regionID, inbox := range rw.inboxes {
		if len(inbox.msgs) == 0 {
			delete(rw.inboxes, regionID)
		} else {
			inbox.reset()
		}
	}
	var reqCount int
	var respCount int
	var raftMsgCount int
	select {
	case <-closeCh:
		for _, applyCh := range rw.applyChs {
			applyCh <- nil
		}
		return true
	case msg := <-rw.raftCh:
		reqCount++
		if msg.Type == MsgTypeRaftMessage {
			raftMsgCount++
		}
		rw.getPeerInbox(msg.RegionID).append(msg)
	case msg := <-rw.applyResCh:
		respCount++
		rw.getPeerInbox(msg.RegionID).append(msg)
	case <-rw.ticker.C:
		rw.pr.peers.Range(func(key, value interface{}) bool {
			regionID := key.(uint64)
			rw.getPeerInbox(regionID).append(NewPeerMsg(MsgTypeTick, regionID, nil))
			return true
		})
	}
	receivingTime := time.Now()
	metrics.WaitMessageDurationHistogram.Observe(receivingTime.Sub(begin).Seconds())
	pending := len(rw.raftCh)
	reqCount += pending
	for i := 0; i < pending; i++ {
		msg := <-rw.raftCh
		if msg.Type == MsgTypeRaftMessage {
			raftMsgCount++
		}
		rw.getPeerInbox(msg.RegionID).append(msg)
	}
	resLen := len(rw.applyResCh)
	respCount += resLen
	for i := 0; i < resLen; i++ {
		msg := <-rw.applyResCh
		rw.getPeerInbox(msg.RegionID).append(msg)
	}
	metrics.RaftBatchSize.Observe(float64(len(rw.inboxes)))
	metrics.ServerGrpcReqBatchSize.Observe(float64(reqCount))
	metrics.ServerGrpcRespBatchSize.Observe(float64(respCount))
	metrics.ServerRaftMessageBatchSize.Observe(float64(raftMsgCount))
	metrics.ReceiveMessageDurationHistogram.Observe(time.Since(receivingTime).Seconds())
	return false
}

func (rw *raftWorker) getPeerInbox(regionID uint64) *peerInbox {
	inbox, ok := rw.inboxes[regionID]
	if !ok {
		peerState := rw.pr.get(regionID)
		inbox = &peerInbox{peer: peerState}
		rw.inboxes[regionID] = inbox
	}
	return inbox
}

func (rw *raftWorker) processInBox(inbox *peerInbox) *ReadyICPair {
	h := newRaftMsgHandler(inbox.peer.peer, rw.raftCtx)
	h.HandleMsgs(inbox.msgs...)
	rd := h.newRaftReady()
	if rd != nil {
		h.HandleRaftReady(&rd.Ready, rd.IC)
		applyMsgs := rw.raftCtx.applyMsgs
		peerBatch := &peerApplyBatch{
			apply:     inbox.peer.apply,
			applyMsgs: append([]Msg{}, applyMsgs.msgs...),
		}
		for i := 0; i < len(applyMsgs.msgs); i++ {
			msg := applyMsgs.msgs[i]
			if msg.Type == MsgTypeApply {
				a := msg.Data.(*apply)
				if len(a.traces) > 0 {
					now := time.Now()
					for _, t := range a.traces {
						t.schedulingApplyTime = now
						metrics.ScheduleApplyWaitTimeDurationHistogram.Observe(now.Sub(t.committedTime).Seconds())
					}
				}
			}
			applyMsgs.msgs[i] = Msg{}
		}
		applyMsgs.msgs = applyMsgs.msgs[:0]
		idx := hashRegionID(inbox.peer.peer.regionID()) % uint64(len(rw.applyChs))
		rw.applyChs[idx] <- peerBatch
	}
	return rd
}

func (rw *raftWorker) postPersistState(readyRes []*ReadyICPair) {
	begin := time.Now()
	for _, pair := range readyRes {
		peer := rw.inboxes[pair.IC.Region.Id].peer.peer
		if !peer.peer.IsLeader() {
			peer.peer.followerSendReadyMessages(rw.raftCtx.trans, &pair.Ready)
		}
	}
	metrics.PostPersistStateDurationHistogram.Observe(time.Since(begin).Seconds())
}

func (rw *raftWorker) persistState() {
	raftWB := rw.raftCtx.raftWB
	if !raftWB.IsEmpty() {
		begin := time.Now()
		err := rw.raftCtx.engine.raft.Write(raftWB)
		if err != nil {
			panic(err)
		}
		rw.raftCtx.localStats.engineTotalKeysWritten += uint64(raftWB.NumEntries())
		rw.raftCtx.localStats.engineTotalBytesWritten += uint64(raftWB.Size())
		raftWB.Reset()
		duration := time.Since(begin)
		rw.writeDc.collect(duration)
		metrics.PeerAppendLogHistogram.Observe(duration.Seconds())
		metrics.PersistStateDurationHistogram.Observe(duration.Seconds())
	}
}

type applyWorker struct {
	idx int
	r   *router
	ch  chan *peerApplyBatch
	ctx *applyContext
	dc  *durationCollector
}

func newApplyWorker(r *router, idx int, ch chan *peerApplyBatch, ctx *applyContext) *applyWorker {
	return &applyWorker{
		idx: idx,
		r:   r,
		ch:  ch,
		ctx: ctx,
		dc:  newDurationCollector(fmt.Sprintf("apply_handle_msg_%d", idx)),
	}
}

// run runs apply tasks, since it is already batched by raftCh, we don't need to batch it here.
func (aw *applyWorker) run(wg *sync.WaitGroup) {
	defer wg.Done()
	applyMetrics := metrics.WorkerPendingTaskTotal.WithLabelValues("apply-worker")
	for {
		batch := <-aw.ch
		if batch == nil {
			return
		}
		for _, msg := range batch.applyMsgs {
			batch.apply.handleMsg(aw.ctx, msg)
		}
		applyMetrics.Set(float64(len(aw.ch)))
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
					msg = NewMsg(MsgTypeStoreTick, StoreTick(i))
				}
			}
		case msg = <-sw.store.receiver:
		}
		metrics.WorkerPendingTaskTotal.WithLabelValues("store-worker").Set(float64(len(sw.store.receiver) + 1))
		sw.store.handleMsg(msg)
		metrics.WorkerHandledTaskTotal.WithLabelValues("store-worker").Inc()
		metrics.WorkerPendingTaskTotal.WithLabelValues("store-worker").Set(float64(len(sw.store.receiver)))
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
	metrics.WorkerTaskDurationSeconds.WithLabelValues(dc.name).Observe(float64(dur) / float64(time.Second))
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
