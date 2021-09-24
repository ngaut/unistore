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
	"bytes"
	"fmt"
	"github.com/ngaut/unistore/engine"
	"github.com/ngaut/unistore/enginepb"
	"sync/atomic"
	"time"

	"github.com/ngaut/unistore/tikv/raftstore/raftlog"

	"github.com/ngaut/unistore/metrics"
	"github.com/ngaut/unistore/raft"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/tablecodec"
)

type peerFsm struct {
	peer     *Peer
	stopped  bool
	hasReady bool
	ticker   *ticker
}

type PeerEventContext struct {
	LeaderChecker LeaderChecker
	RegionId      uint64
}

type PeerEventObserver interface {
	// OnPeerCreate will be invoked when there is a new peer created.
	OnPeerCreate(ctx *PeerEventContext, region *metapb.Region)
	// OnPeerApplySnap will be invoked when there is a replicate peer's snapshot applied.
	OnPeerApplySnap(ctx *PeerEventContext, region *metapb.Region)
	// OnPeerDestroy will be invoked when a peer is destroyed.
	OnPeerDestroy(ctx *PeerEventContext)
	// OnSplitRegion will be invoked when region split into new regions with corresponding peers.
	OnSplitRegion(derived *metapb.Region, regions []*metapb.Region, peers []*PeerEventContext)
	// OnRegionConfChange will be invoked after conf change updated region's epoch.
	OnRegionConfChange(ctx *PeerEventContext, epoch *metapb.RegionEpoch)
	// OnRoleChange will be invoked after peer state has changed
	OnRoleChange(regionId uint64, newState raft.StateType)
}

// If we create the peer actively, like bootstrap/split/merge region, we should
// use this function to create the peer. The region must contain the peer info
// for this store.
func createPeerFsm(storeID uint64, cfg *Config, sched chan<- task,
	engines *Engines, region *metapb.Region) (*peerFsm, error) {
	metaPeer := findPeer(region, storeID)
	if metaPeer == nil {
		return nil, errors.Errorf("find no peer for store %d in region %v", storeID, region)
	}
	log.S().Infof("region %d:%d create peer with ID %d", region, region.RegionEpoch.Version, metaPeer.Id)
	peer, err := NewPeer(storeID, cfg, engines, region, sched, metaPeer)
	if err != nil {
		return nil, err
	}
	return &peerFsm{
		peer:   peer,
		ticker: newTicker(region.GetId(), cfg),
	}, nil
}

// The peer can be created from another node with raft membership changes, and we only
// know the region_id and peer_id when creating this replicated peer, the region info
// will be retrieved later after applying snapshot.
func replicatePeerFsm(storeID uint64, cfg *Config, sched chan<- task,
	engines *Engines, regionID uint64, startKey, endKey []byte, metaPeer *metapb.Peer) (*peerFsm, error) {
	// We will remove tombstone key when apply snapshot
	log.S().Infof("[region %v] replicates peer with ID %d", regionID, metaPeer.GetId())
	region := &metapb.Region{
		Id:          regionID,
		StartKey:    startKey,
		EndKey:      endKey,
		RegionEpoch: &metapb.RegionEpoch{},
	}
	peer, err := NewPeer(storeID, cfg, engines, region, sched, metaPeer)
	if err != nil {
		return nil, err
	}
	return &peerFsm{
		peer:   peer,
		ticker: newTicker(region.GetId(), cfg),
	}, nil
}

func (pf *peerFsm) drop() {
	pf.peer.Stop()
}

func (pf *peerFsm) regionID() uint64 {
	return pf.peer.regionId
}

func (pf *peerFsm) region() *metapb.Region {
	return pf.peer.Store().region
}

func (pf *peerFsm) getPeer() *Peer {
	return pf.peer
}

func (pf *peerFsm) storeID() uint64 {
	return pf.peer.Meta.StoreId
}

func (pf *peerFsm) peerID() uint64 {
	return pf.peer.Meta.Id
}

func (pf *peerFsm) stop() {
	pf.stopped = true
}

func (pf *peerFsm) setPendingMergeState(state *rspb.MergeState) {
	pf.peer.PendingMergeState = state
}

func (pf *peerFsm) hasPendingMergeApplyResult() bool {
	return pf.peer.PendingMergeApplyResult != nil
}

func (pf *peerFsm) tag() string {
	return pf.peer.Tag
}

type peerMsgHandler struct {
	*peerFsm
	ctx *RaftContext
}

func newRaftMsgHandler(fsm *peerFsm, ctx *RaftContext) *peerMsgHandler {
	return &peerMsgHandler{
		peerFsm: fsm,
		ctx:     ctx,
	}
}

func (d *peerMsgHandler) HandleMsgs(msgs ...Msg) {
	for _, msg := range msgs {
		switch msg.Type {
		case MsgTypeRaftMessage:
			raftMsg := msg.Data.(*rspb.RaftMessage)
			if err := d.onRaftMsg(raftMsg); err != nil {
				log.S().Errorf("%s handle raft message error %v", d.peer.Tag, err)
			}
		case MsgTypeRaftCmd:
			raftCMD := msg.Data.(*MsgRaftCmd)
			d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
		case MsgTypeTick:
			d.onTick()
		case MsgTypeApplyRes:
			res := msg.Data.(*applyTaskRes)
			if state := d.peer.PendingMergeApplyResult; state != nil {
				state.results = append(state.results, res)
				continue
			}
			d.onApplyResult(res)
		case MsgTypeSplitRegion:
			split := msg.Data.(*MsgSplitRegion)
			log.S().Infof("%s on split with %v", d.peer.Tag, split.SplitKeys)
			d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKeys, split.Callback)
		case MsgTypeComputeResult:
			result := msg.Data.(*MsgComputeHashResult)
			d.onHashComputed(result.Index, result.Hash)
		case MsgTypeHalfSplitRegion:
			half := msg.Data.(*MsgHalfSplitRegion)
			d.onScheduleHalfSplitRegion(half.RegionEpoch)
		case MsgTypeMergeResult:
			result := msg.Data.(*MsgMergeResult)
			d.onMergeResult(result.TargetPeer, result.Stale)
		case MsgTypeStart:
			d.startTicker()
		case MsgTypeNoop:
		case MsgTypeGenerateEngineChangeSet:
			d.onGenerateMetaChangeEvent(msg.Data.(*enginepb.ChangeSet))
		case MsgTypeWaitFollowerSplitFiles:
			d.peer.waitFollowerSplitFiles = msg.Data.(*MsgWaitFollowerSplitFiles)
		case MsgTypeApplyChangeSetResult:
			d.onApplyChangeSetResult(msg.Data.(*MsgApplyChangeSetResult))
		}
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickPdHeartbeat) {
		d.onPDHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	if d.ticker.isOnTick(PeerTickCheckMerge) {
		d.onCheckMerge()
	}
	if d.ticker.isOnTick(PeerTickPeerStaleState) {
		d.onCheckPeerStaleStateTick()
	}
}

func (d *peerMsgHandler) startTicker() {
	if d.peer.PendingMergeState != nil {
		d.notifyPrepareMerge()
	}
	d.ticker = newTicker(d.regionID(), d.ctx.cfg)
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickPdHeartbeat)
	d.ticker.schedule(PeerTickPeerStaleState)
	d.onCheckMerge()
}

func (d *peerMsgHandler) notifyPrepareMerge() {
	// TODO: merge func
}

func (d *peerMsgHandler) resumeHandlePendingApplyResult() bool {
	return false // TODO: merge func
}

func (d *peerMsgHandler) newRaftReady() *ReadyICPair {
	hasReady := d.hasReady
	d.hasReady = false
	if !hasReady || d.stopped {
		return nil
	}
	readyRes := d.peer.NewRaftReady(d.ctx.trans, d.ctx, d.ctx.peerEventObserver)
	if readyRes != nil {
		ss := readyRes.Ready.SoftState
		if ss != nil && ss.RaftState == raft.StateLeader {
			d.peer.HeartbeatPd(d.ctx.pdTaskSender)
		}
	}
	return readyRes
}

func (d *peerMsgHandler) HandleRaftReady(ready *raft.Ready, ic *InvokeContext) {
	if p := d.peer.TakeApplyProposals(); p != nil {
		msg := Msg{Type: MsgTypeApplyProposal, Data: p}
		d.ctx.applyMsgs.appendMsg(p.RegionId, msg)
		metrics.RaftstoreApplyProposal.Observe(float64(len(p.Props)))
	}
	d.peer.Store().updateStates(ic)
	readyApplySnapshot := d.peer.Store().maybeScheduleApplySnapshot(ic)
	if readyApplySnapshot != nil {
		// The peer may change from learner to voter after snapshot applied.
		d.peer.maybeUpdatePeerMeta()
		d.peer.Activate(d.ctx.applyMsgs)
		d.onReadyApplySnapshot(readyApplySnapshot)
		if d.peer.PendingMergeState != nil {
			// After applying a snapshot, merge is rollbacked implicitly.
			d.onReadyRollbackMerge(0, nil)
		}
	}
	d.peer.HandleRaftReadyApplyMessages(d.ctx.engine.kv, d.ctx.applyMsgs, ready)
	if d.peer.waitFollowerSplitFiles != nil {
		if d.peer.Store().splitStage == enginepb.SplitStage_SPLIT_FILE_DONE {
			epochVer := d.region().RegionEpoch.Version
			matchCnt := 0
			for _, followerVer := range d.peer.followersSplitFilesDone {
				if followerVer == epochVer {
					matchCnt++
				}
			}
			if matchCnt == len(d.region().Peers)-1 {
				log.S().Infof("%d:%d leader schedule finish split", d.region().Id, epochVer)
				d.ctx.regionTaskSender <- task{
					tp: taskTypeFinishSplit,
					data: &regionTask{
						region:    d.region(),
						waitMsg:   d.peer.waitFollowerSplitFiles,
						startTime: time.Now(),
					},
				}
				d.peer.waitFollowerSplitFiles = nil
			}
		}
	}
}

func getApplyStateFromProps(props *enginepb.Properties) applyState {
	val, ok := engine.GetShardProperty(applyStateKey, props)
	y.Assert(ok)
	var applyState applyState
	applyState.Unmarshal(val)
	return applyState
}

func (d *peerMsgHandler) onRaftBaseTick() {
	if d.peer.PendingRemove {
		return
	}
	// When having pending snapshot, if election timeout is met, it can't pass
	// the pending conf change check because first index has been updated to
	// a value that is larger than last index.
	if d.peer.IsApplyingSnapshot() || d.peer.HasPendingSnapshot() {
		// need to check if snapshot is applied.
		d.hasReady = true
		d.ticker.schedule(PeerTickRaft)
		return
	}
	// TODO: make Tick returns bool to indicate if there is ready.
	d.peer.RaftGroup.Tick()
	d.hasReady = d.peer.RaftGroup.HasReady()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) onApplyResult(res *applyTaskRes) {
	if res.destroyPeerID != 0 {
		y.AssertTruef(res.destroyPeerID == d.peerID(), "region %d:%d peer %d, destroy wrong peer %d", d.regionID(), d.region().RegionEpoch.Version, d.peerID(), res.destroyPeerID)
		d.destroyPeer(false)
	} else {
		var readyToMerge *uint32
		readyToMerge, res.execResults = d.onReadyResult(res.merged, res.execResults)
		if readyToMerge != nil {
			// There is a `CommitMerge` needed to wait
			d.peer.PendingMergeApplyResult = &WaitApplyResultState{
				results:      []*applyTaskRes{res},
				readyToMerge: readyToMerge,
			}
			return
		}
		if d.stopped {
			return
		}
		if d.peer.PostApply(d.ctx.engine.kv, res.applyState, res.merged, res.metrics) {
			d.hasReady = true
		}
		if res.metrics.writtenBytes > 0 {
			atomic.AddUint64(&d.ctx.globalStats.engineTotalBytesWritten, res.metrics.writtenBytes)
			metrics.EngineFlowBytes.WithLabelValues("kv", "bytes_written").Add(float64(res.metrics.writtenBytes))
		}
		if res.metrics.writtenKeys > 0 {
			atomic.AddUint64(&d.ctx.globalStats.engineTotalKeysWritten, res.metrics.writtenKeys)
			metrics.EngineFlowBytes.WithLabelValues("kv", "keys_written").Add(float64(res.metrics.writtenKeys))
		}
	}
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.peer.PendingRemove || d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if msg.MergeTarget != nil {
		need, err := d.needGCMerge(msg)
		if err != nil {
			return err
		}
		if need {
			d.onStaleMerge()
		}
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	d.checkPendingSplit(msg)
	d.peer.insertPeerCache(msg.GetFromPeer())
	err = d.peer.Step(msg.GetMessage())
	if err != nil {
		return err
	}
	if d.peer.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.peer.HeartbeatPd(d.ctx.pdTaskSender)
	}
	d.updateFollowerSplitFilesDone(msg)
	d.hasReady = true
	return nil
}

func (d *peerMsgHandler) checkPendingSplit(msg *rspb.RaftMessage) {
	for _, e := range msg.Message.Entries {
		if e.EntryType == eraftpb.EntryType_EntryNormal && len(e.Data) > 0 {
			splits := raftlog.TryGetSplit(e.Data)
			if splits != nil {
				d.peer.PendingSplit = true
			}
		}
	}
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	to := msg.GetToPeer()
	if to.GetStoreId() != d.storeID() {
		log.S().Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.S().Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := isVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with pd and pd will
	// tell 2 is stale, so 2 can remove itself.
	region := d.peer.Region()
	if IsEpochStale(fromEpoch, region.RegionEpoch) && findPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg, nil)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.peerID() {
		log.S().Infof("%s target peer ID %d is less than %d, msg maybe stale", d.tag(), target.Id, d.peerID())
		return true
	} else if target.Id > d.peerID() {
		if job := d.peer.MaybeDestroy(); job != nil {
			log.S().Infof("%s is stale as received a larger peer %s, destroying", d.tag(), target)
			if d.handleDestroyPeer(job) {
				storeMsg := NewMsg(MsgTypeStoreRaftMessage, msg)
				d.ctx.router.sendStore(storeMsg)
			}
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool, targetRegion *metapb.Region) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.S().Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    fromPeer,
		ToPeer:      toPeer,
		RegionEpoch: curEpoch,
	}
	if targetRegion != nil {
		gcMsg.MergeTarget = targetRegion
	} else {
		gcMsg.IsTombstone = true
	}
	trans.Send(gcMsg)
}

func (d *peerMsgHandler) needGCMerge(msg *rspb.RaftMessage) (bool, error) {
	return false, nil // TODO: merge func
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !IsEpochStale(d.peer.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !PeerEqual(d.peer.Meta, msg.ToPeer) {
		log.S().Infof("%s receive stale gc msg, ignore", d.tag())
		return
	}
	// TODO: ask pd to guarantee we are stale now.
	log.S().Infof("%s peer %s receives gc message, trying to remove", d.tag(), msg.ToPeer)
	if job := d.peer.MaybeDestroy(); job != nil {
		d.handleDestroyPeer(job)
	}
}

func u64SliceContains(slice []uint64, v uint64) bool {
	for _, e := range slice {
		if e == v {
			return true
		}
	}
	return false
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) error {
	if msg.Message.Snapshot == nil {
		return nil
	}
	snap := msg.Message.Snapshot
	confState := snap.Metadata.ConfState
	peerID := msg.ToPeer.Id
	contains := u64SliceContains(confState.Voters, peerID) || u64SliceContains(confState.Learners, peerID)
	if !contains {
		log.S().Infof("%s %v doesn't contains peer %d, skip", d.tag(), msg.RegionId, peerID)
		return nil
	}
	var regionsToDestroy []uint64
	d.ctx.storeMetaLock.Lock()
	defer func() {
		d.ctx.storeMetaLock.Unlock()
		// destroy regions out of lock to avoid dead lock.
		destroyRegions(d.ctx.router, regionsToDestroy, d.getPeer().Meta)
	}()
	meta := d.ctx.storeMeta
	if !RegionEqual(meta.regions[d.regionID()], d.region()) {
		if !d.peer.isInitialized() {
			log.S().Infof("%s stale delegate detected, skip", d.tag())
			return nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.tag(), meta.regions[d.regionID()], d.region()))
		}
	}

	// In some extreme cases, it may cause source peer destroyed improperly so that a later
	// CommitMerge may panic because source is already destroyed, so just drop the message:
	// 1. A new snapshot is received whereas a snapshot is still in applying, and the snapshot
	// under applying is generated before merge and the new snapshot is generated after merge.
	// After the applying snapshot is finished, the log may able to catch up and so a
	// CommitMerge will be applied.
	// 2. There is a CommitMerge pending in apply thread.
	ready := !d.peer.IsApplyingSnapshot() && !d.peer.HasPendingSnapshot() && d.peer.ReadyToHandlePendingSnap()

	existRegions := d.findOverlapRegions(meta, msg.RegionId, msg.StartKey, msg.EndKey)
	for _, existRegion := range existRegions {
		log.S().Infof("%s region overlapped %s %d", d.tag(), existRegion, msg.RegionId)
		if ready && maybeDestroySource(meta, d.regionID(), existRegion.Id, msg.RegionEpoch) {
			// The snapshot that we decide to whether destroy peer based on must can be applied.
			// So here not to destroy peer immediately, or the snapshot maybe dropped in later
			// check but the peer is already destroyed.
			regionsToDestroy = append(regionsToDestroy, existRegion.Id)
			continue
		}
		return nil
	}
	return nil
}

func (d *peerMsgHandler) findOverlapRegions(storeMeta *storeMeta, id uint64, startKey, endKey []byte) (result []*metapb.Region) {
	storeMeta.regionTree.Iterate(startKey, endKey, func(region *metapb.Region) bool {
		if region.Id != id {
			result = append(result, region)
		}
		return true
	})
	return
}

func (d *peerMsgHandler) handleDestroyPeer(job *DestroyPeerJob) bool {
	if job.Initialized {
		d.ctx.applyMsgs.appendMsg(job.RegionId, NewPeerMsg(MsgTypeApplyDestroy, job.RegionId, nil))
	}
	if job.AsyncRemove {
		log.S().Infof("[region %d] %d is destroyed asynchronously", job.RegionId, job.Peer.Id)
		return false
	}
	d.destroyPeer(false)
	return true
}

func (d *peerMsgHandler) destroyPeer(mergeByTarget bool) {
	log.S().Infof("%s starts destroy [merged_by_target: %v]", d.tag(), mergeByTarget)
	regionID := d.regionID()
	// We can't destroy a peer which is applying snapshot.
	y.Assert(!d.peer.IsApplyingSnapshot())
	d.ctx.storeMetaLock.Lock()
	defer func() {
		d.ctx.storeMetaLock.Unlock()
		// send messages out of store meta lock.
		d.ctx.applyMsgs.appendMsg(regionID, NewPeerMsg(MsgTypeApplyDestroy, regionID, nil))
		d.ctx.pdTaskSender <- task{
			tp: taskTypePDDestroyPeer,
			data: &pdDestroyPeerTask{
				regionID: regionID,
			},
		}
	}()
	meta := d.ctx.storeMeta
	delete(meta.pendingMergeTargets, regionID)
	if targetID, ok := meta.targetsMap[regionID]; ok {
		delete(meta.targetsMap, regionID)
		if target, ok1 := meta.pendingMergeTargets[targetID]; ok1 {
			delete(target, regionID)
			// When the target doesn't exist(add peer but the store is isolated), source peer decide to destroy by itself.
			// Without target, the `pending_merge_targets` for target won't be removed, so here source peer help target to clear.
			if meta.regions[targetID] == nil && len(meta.pendingMergeTargets[targetID]) == 0 {
				delete(meta.pendingMergeTargets, targetID)
			}
		}
	}
	delete(meta.mergeLocks, regionID)
	isInitialized := d.peer.isInitialized()
	if err := d.peer.Destroy(d.ctx.engine, mergeByTarget); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.tag(), err))
	}
	d.ctx.router.close(regionID)
	d.stop()
	if isInitialized && !mergeByTarget && !meta.regionTree.Delete(d.region()) {
		meta.regionTree.Iterate(nil, nil, func(region *metapb.Region) bool {
			log.S().Infof("existing region %s", region)
			return true
		})

		panic(d.tag() + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok && !mergeByTarget {
		panic(d.tag() + " meta corruption detected")
	}
	delete(meta.regions, regionID)
	d.ctx.engine.kv.RemoveShard(regionID, false)
	d.ctx.peerEventObserver.OnPeerDestroy(d.peer.getEventContext())
}

func (d *peerMsgHandler) onReadyChangePeer(cp changePeer) {
	if cp.confChange == nil {
		log.S().Warnf("%s conf change is aborted", d.tag())
		return
	}
	changeType := cp.confChange.ChangeType
	d.peer.RaftGroup.ApplyConfChange(*cp.confChange)
	if cp.confChange.NodeId == 0 {
		// Apply failed, skip.
		return
	}
	d.ctx.storeMetaLock.Lock()
	d.ctx.storeMeta.setRegion(cp.region, d.peer)
	d.ctx.storeMetaLock.Unlock()
	d.ctx.peerEventObserver.OnRegionConfChange(d.peer.getEventContext(), &metapb.RegionEpoch{
		ConfVer: cp.region.RegionEpoch.ConfVer,
		Version: cp.region.RegionEpoch.Version,
	})
	peerID := cp.peer.Id
	state := rspb.PeerState_Normal
	switch changeType {
	case eraftpb.ConfChangeType_AddNode, eraftpb.ConfChangeType_AddLearnerNode:
		if d.peerID() == peerID && d.peer.Meta.Role == metapb.PeerRole_Learner {
			d.peer.Meta = cp.peer
		}

		// Add this peer to cache and heartbeats.
		now := time.Now()
		d.peer.PeerHeartbeats[peerID] = now
		if d.peer.IsLeader() {
			d.peer.PeersStartPendingTime[peerID] = now
		}
		d.peer.RecentAddedPeer.Update(peerID, now)
		d.peer.insertPeerCache(cp.peer)
	case eraftpb.ConfChangeType_RemoveNode:
		// Remove this peer from cache.
		delete(d.peer.PeerHeartbeats, peerID)
		if d.peer.IsLeader() {
			delete(d.peer.PeersStartPendingTime, peerID)
		}
		delete(d.peer.followersSplitFilesDone, peerID)
		d.peer.removePeerCache(peerID)
		if d.peerID() == peerID {
			state = rspb.PeerState_Tombstone
		}
		log.S().Infof("region %d:%d remove node [store %d peer %d] from node [store %d peer %d]",
			d.regionID(), d.region().RegionEpoch.Version, cp.peer.StoreId, cp.peer.Id, d.storeID(), d.peerID())
	}
	WritePeerState(d.ctx.raftWB, cp.region, state, nil)

	// In pattern matching above, if the peer is the leader,
	// it will push the change peer into `peers_start_pending_time`
	// without checking if it is duplicated. We move `heartbeat_pd` here
	// to utilize `collect_pending_peers` in `heartbeat_pd` to avoid
	// adding the redundant peer.
	if d.peer.IsLeader() {
		// Notify pd immediately.
		log.S().Infof("%s notify pd with change peer region %s", d.tag(), d.region())
		d.peer.HeartbeatPd(d.ctx.pdTaskSender)
	}
	myPeerID := d.peerID()

	// We only care remove itself now.
	if changeType == eraftpb.ConfChangeType_RemoveNode && cp.peer.StoreId == d.storeID() {
		if myPeerID == peerID {
			d.destroyPeer(false)
		} else {
			panic(fmt.Sprintf("%s trying to remove unknown peer %s", d.tag(), cp.peer))
		}
	}
}

func (d *peerMsgHandler) onReadyCompactLog(firstIndex uint64, truncatedIndex uint64) {
	d.ctx.raftWB.TruncateRaftLog(d.regionID(), truncatedIndex)
	d.peer.LastCompactedIdx = truncatedIndex
}

func (d *peerMsgHandler) onReadySplitRegion(derived *metapb.Region, regions []*metapb.Region) {
	for _, region := range regions {
		WritePeerState(d.ctx.raftWB, region, rspb.PeerState_Normal, nil)
	}
	d.ctx.storeMetaLock.Lock()
	defer d.ctx.storeMetaLock.Unlock()
	meta := d.ctx.storeMeta
	regionID := derived.Id
	meta.setRegion(derived, d.getPeer())
	d.peer.PendingSplit = false
	isLeader := d.peer.IsLeader()
	if isLeader {
		d.peer.HeartbeatPd(d.ctx.pdTaskSender)
		// Notify pd immediately to let it update the region meta.
		log.S().Infof("%s notify pd with split count %d", d.tag(), len(regions))
		// Now pd only uses ReportBatchSplit for history operation show,
		// so we send it independently here.
		d.ctx.pdTaskSender <- task{
			tp:   taskTypePDReportBatchSplit,
			data: &pdReportBatchSplitTask{regions: regions},
		}
	}

	lastRegion := regions[len(regions)-1]
	if !meta.regionTree.Delete(lastRegion) {
		panic(d.tag() + " original region should exist")
	}

	newPeers := make([]*PeerEventContext, 0, len(regions))
	for _, newRegion := range regions {
		newRegionID := newRegion.Id
		notExist := meta.regionTree.Put(newRegion)
		y.Assert(notExist)
		if newRegionID == regionID {
			newPeers = append(newPeers, d.peer.getEventContext())
			store := d.peer.Store()
			// The raft state key changed when region version change, we need to set it here.
			y.Assert(store.raftState.commit > 0)
			d.ctx.raftWB.SetState(regionID, RaftStateKey(d.region().RegionEpoch.Version), store.raftState.Marshal())
			// Reset the flush state for derived region.
			store.initialFlushed = false
			store.splitStage = enginepb.SplitStage_INITIAL
			continue
		}

		// Insert new regions and validation
		log.S().Infof("[region %d:%d] inserts new region %d:%d", derived.Id, d.region().RegionEpoch.Version, newRegion.Id, newRegion.RegionEpoch.Version)
		if r, ok := meta.regions[newRegionID]; ok {
			// Suppose a new node is added by conf change and the snapshot comes slowly.
			// Then, the region splits and the first vote message comes to the new node
			// before the old snapshot, which will create an uninitialized peer on the
			// store. After that, the old snapshot comes, followed with the last split
			// proposal. After it's applied, the uninitialized peer will be met.
			// We can remove this uninitialized peer directly.
			if len(r.Peers) > 0 {
				panic(fmt.Sprintf("[region %d] duplicated region %s for split region %s",
					newRegionID, r, newRegion))
			}
			d.ctx.router.close(newRegionID)
		}

		newPeer, err := createPeerFsm(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			// peer information is already written into db, can't recover.
			// there is probably a bug.
			panic(fmt.Sprintf("create new split region %s error %v", newRegion, err))
		}
		metaPeer := newPeer.peer.Meta
		newPeers = append(newPeers, newPeer.peer.getEventContext())

		for _, p := range newRegion.GetPeers() {
			newPeer.peer.insertPeerCache(p)
		}

		// New peer derive write flow from parent region,
		// this will be used by balance write flow.
		newPeer.peer.PeerStat = d.peer.PeerStat
		campaigned := newPeer.peer.MaybeCampaign(isLeader)
		newPeer.hasReady = newPeer.hasReady || campaigned

		if isLeader {
			// The new peer is likely to become leader, send a heartbeat immediately to reduce
			// client query miss.
			newPeer.peer.HeartbeatPd(d.ctx.pdTaskSender)
		}

		newPeer.peer.Activate(d.ctx.applyMsgs)
		meta.regions[newRegionID] = newRegion
		d.ctx.router.register(newPeer)
		_ = d.ctx.router.send(newRegionID, NewPeerMsg(MsgTypeStart, newRegionID, nil))
		if !campaigned {
			for i, msg := range meta.pendingVotes {
				if PeerEqual(msg.ToPeer, metaPeer) {
					meta.pendingVotes = append(meta.pendingVotes[:i], meta.pendingVotes[i+1:]...)
					_ = d.ctx.router.send(newRegionID, NewPeerMsg(MsgTypeRaftMessage, newRegionID, msg))
					break
				}
			}
		}
	}
	d.ctx.peerEventObserver.OnSplitRegion(derived, regions, newPeers)
}

func (d *peerMsgHandler) validateMergePeer(targetRegion *metapb.Region) (bool, error) {
	return false, nil // TODO: merge func
}

func (d *peerMsgHandler) scheduleMerge() error {
	return nil // TODO: merge func
}

func (d *peerMsgHandler) rollbackMerge() {
	// TODO: merge func
}

func (d *peerMsgHandler) onCheckMerge() {
	// TODO: merge func
}

func (d *peerMsgHandler) onReadyPrepareMerge(region *metapb.Region, state *rspb.MergeState, merged bool) {
	// TODO: merge func
}

func (d *peerMsgHandler) onReadyCommitMerge(region, source *metapb.Region) *uint32 {
	return nil // TODO: merge func
}

func (d *peerMsgHandler) onReadyRollbackMerge(commit uint64, region *metapb.Region) {
	// TODO: merge func
}

func (d *peerMsgHandler) onMergeResult(target *metapb.Peer, stale bool) {
	// TODO: merge func
}

func (d *peerMsgHandler) onStaleMerge() {
	// TODO: merge func
}

func (d *peerMsgHandler) onReadyApplySnapshot(applyResult *ReadyApplySnapshot) {
	prevRegion := applyResult.PrevRegion
	region := applyResult.Region

	log.S().Infof("%s snapshot for region %s is applied", d.tag(), region)
	d.ctx.storeMetaLock.Lock()
	defer d.ctx.storeMetaLock.Unlock()
	meta := d.ctx.storeMeta
	initialized := len(prevRegion.Peers) > 0
	if initialized {
		log.S().Infof("%s region changed from %s -> %s after applying snapshot", d.tag(), prevRegion, region)
		meta.regionTree.Delete(prevRegion)
	}
	if !meta.regionTree.Put(region) {
		oldRegion := meta.regionTree.GetRegionByKey(region.StartKey)
		panic(fmt.Sprintf("%s unexpected old region %d", d.tag(), oldRegion.Id))
	}
	meta.regions[region.Id] = region
	d.ctx.peerEventObserver.OnPeerApplySnap(d.peer.getEventContext(), region)
}

func (d *peerMsgHandler) onReadyResult(merged bool, execResults []execResult) (*uint32, []execResult) {
	if len(execResults) == 0 {
		return nil, nil
	}

	// handle executing committed log results
	for i, result := range execResults {
		switch x := result.(type) {
		case *execResultChangePeer:
			d.onReadyChangePeer(x.cp)
		case *execResultCompactLog:
			if !merged {
				d.onReadyCompactLog(x.firstIndex, x.truncatedIndex)
			}
		case *execResultSplitRegion:
			d.onReadySplitRegion(x.derived, x.regions)
		case *execResultPrepareMerge:
			d.onReadyPrepareMerge(x.region, x.state, merged)
		case *execResultCommitMerge:
			if readyToMerge := d.onReadyCommitMerge(x.region, x.source); readyToMerge != nil {
				return readyToMerge, execResults[i:]
			}
		case *execResultRollbackMerge:
			d.onReadyRollbackMerge(x.commit, x.region)
		case *execResultComputeHash:
			d.onReadyComputeHash(x.region, x.index, x.snap)
		case *execResultVerifyHash:
			d.onReadyVerifyHash(x.index, x.hash)
		case *execResultDeleteRange:
			// TODO: clean user properties?
		}
	}
	return nil, nil
}

func (d *peerMsgHandler) checkMergeProposal(msg *raft_cmdpb.RaftCmdRequest) error {
	return nil // TODO: merge func
}

func (d *peerMsgHandler) preProposeRaftCommand(rlog raftlog.RaftLog) (*raft_cmdpb.RaftCmdResponse, error) {
	req := rlog.GetRaftCmdRequest()
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := checkStoreID(rlog, d.storeID()); err != nil {
		return nil, err
	}
	if req.GetStatusRequest() != nil {
		// For status commands, we handle it here directly.
		return d.executeStatusCommand(req)
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionID()
	leaderID := d.peer.LeaderId()
	if !d.peer.IsLeader() {
		leader := d.peer.getPeerFromCache(leaderID)
		return nil, &ErrNotLeader{regionID, leader}
	}
	// peer_id must be the same as peer's.
	if err := checkPeerID(rlog, d.peerID()); err != nil {
		return nil, err
	}
	// Check whether the term is stale.
	if err := checkTerm(rlog, d.peer.Term()); err != nil {
		return nil, err
	}
	return nil, checkRegionEpoch(rlog, d.region(), true)
}

func (d *peerMsgHandler) proposeRaftCommand(rlog raftlog.RaftLog, cb *Callback) {
	resp, err := d.preProposeRaftCommand(rlog)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	if resp != nil {
		cb.Done(resp)
		return
	}

	if d.peer.PendingRemove {
		NotifyReqRegionRemoved(d.regionID(), cb)
		return
	}
	msg := rlog.GetRaftCmdRequest()
	if err := d.checkMergeProposal(msg); err != nil {
		log.S().Warnf("%s failed to process merge, message %s, err %v", d.tag(), msg, err)
		cb.Done(ErrResp(err))
		return
	}

	// Note:
	// The peer that is being checked is a leader. It might step down to be a follower later. It
	// doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
	// command log entry can't be committed.

	resp = &raft_cmdpb.RaftCmdResponse{}
	BindRespTerm(resp, d.peer.Term())
	if d.peer.Propose(d.ctx.engine.kv, d.ctx.cfg, cb, rlog, resp) {
		d.hasReady = true
	}

	// TODO: add timeout, if the command is not applied after timeout,
	// we will call the callback with timeout error.
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	store := d.peer.Store()
	stableIdx := store.stableApplyState.appliedIndex
	if stableIdx == 0 {
		// No flushed L0 files yet, we delay the raft log GC.
		return
	}
	if !d.peer.IsLeader() {
		return
	}
	term, err := d.peer.RaftGroup.Raft.RaftLog.Term(stableIdx)
	if err != nil {
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionID()
	request := newCompactLogRequest(regionID, d.peer.Meta, stableIdx, term)
	d.proposeRaftCommand(raftlog.NewRequest(request), nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)

	if !d.peer.IsLeader() {
		return
	}

	shard := d.ctx.engine.kv.GetShard(d.regionID())
	if shard == nil {
		return
	}
	if shard.GetSplitStage() != enginepb.SplitStage_INITIAL {
		// Avoid repeated split checks.
		return
	}
	estimatedSize := shard.GetEstimatedSize()
	if uint64(estimatedSize) < d.ctx.cfg.SplitCheck.RegionMaxSize {
		return
	}
	select {
	case d.ctx.splitCheckTaskSender <- task{
		tp: taskTypeSplitCheck,
		data: &splitCheckTask{
			region: d.region(),
			peer:   d.peer.Meta,
		},
	}:
	default:
	}

}

func isTableKey(key []byte) bool {
	return bytes.HasPrefix(key, tablecodec.TablePrefix())
}

func isSameTable(leftKey, rightKey []byte) bool {
	return bytes.HasPrefix(leftKey, tablecodec.TablePrefix()) &&
		bytes.HasPrefix(rightKey, tablecodec.TablePrefix()) &&
		len(leftKey) >= tablecodec.TableSplitKeyLen &&
		len(rightKey) >= tablecodec.TableSplitKeyLen &&
		bytes.Compare(leftKey[:tablecodec.TableSplitKeyLen], rightKey[:tablecodec.TableSplitKeyLen]) == 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKeys [][]byte, cb *Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKeys); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.region()
	d.ctx.pdTaskSender <- task{
		tp: taskTypePDAskBatchSplit,
		data: &pdAskBatchSplitTask{
			region:      region,
			splitKeys:   splitKeys,
			peer:        d.peer.Meta,
			rightDerive: true,
			callback:    cb,
		},
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKeys [][]byte) error {
	if len(splitKeys) == 0 {
		err := errors.Errorf("%s no split key is specified", d.tag())
		log.S().Error(err)
		return err
	}
	for _, key := range splitKeys {
		if len(key) == 0 {
			err := errors.Errorf("%s split key should not be empty", d.tag())
			log.S().Error(err)
			return err
		}
	}
	if !d.peer.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.S().Infof("%s not leader, skip", d.tag())
		return &ErrNotLeader{
			RegionId: d.regionID(),
			Leader:   d.peer.getPeerFromCache(d.peer.LeaderId()),
		}
	}

	region := d.region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to PD.
	if latestEpoch.Version != epoch.Version {
		log.S().Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.tag(), latestEpoch, epoch)
		return &ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.tag(), latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onScheduleHalfSplitRegion(regionEpoch *metapb.RegionEpoch) {
	if !d.peer.IsLeader() {
		return
	}
	region := d.region()
	if IsEpochStale(regionEpoch, region.RegionEpoch) {
		log.S().Warnf("%s receive a stale halfsplit message", d.tag())
		return
	}
	d.ctx.splitCheckTaskSender <- task{
		tp: taskTypeHalfSplitCheck,
		data: &splitCheckTask{
			region: region,
			peer:   d.peer.Meta,
		},
	}
}

func (d *peerMsgHandler) onPDHeartbeatTick() {
	d.ticker.schedule(PeerTickPdHeartbeat)
	d.peer.CheckPeers()

	if !d.peer.IsLeader() {
		return
	}
	d.peer.HeartbeatPd(d.ctx.pdTaskSender)
}

func (d *peerMsgHandler) onCheckPeerStaleStateTick() {
	if d.peer.PendingRemove {
		return
	}
	d.ticker.schedule(PeerTickPeerStaleState)

	if d.peer.IsApplyingSnapshot() || d.peer.HasPendingSnapshot() {
		return
	}

	// If this peer detects the leader is missing for a long long time,
	// it should consider itself as a stale peer which is removed from
	// the original cluster.
	// This most likely happens in the following scenario:
	// At first, there are three peer A, B, C in the cluster, and A is leader.
	// Peer B gets down. And then A adds D, E, F into the cluster.
	// Peer D becomes leader of the new cluster, and then removes peer A, B, C.
	// After all these peer in and out, now the cluster has peer D, E, F.
	// If peer B goes up at this moment, it still thinks it is one of the cluster
	// and has peers A, C. However, it could not reach A, C since they are removed
	// from the cluster or probably destroyed.
	// Meantime, D, E, F would not reach B, since it's not in the cluster anymore.
	// In this case, peer B would notice that the leader is missing for a long time,
	// and it would check with pd to confirm whether it's still a member of the cluster.
	// If not, it destroys itself as a stale peer which is removed out already.
	state := d.peer.CheckStaleState(d.ctx.cfg)
	switch state {
	case StaleStateValid:
	case StaleStateLeaderMissing:
		log.S().Warnf("%s leader missing longer than abnormal_leader_missing_duration %v",
			d.tag(), d.ctx.cfg.AbnormalLeaderMissingDuration)
	case StaleStateToValidate:
		// for peer B in case 1 above
		log.S().Warnf("%s leader missing longer than max_leader_missing_duration %v. To check with pd whether it's still valid",
			d.tag(), d.ctx.cfg.AbnormalLeaderMissingDuration)
		d.ctx.pdTaskSender <- task{
			tp: taskTypePDValidatePeer,
			data: &pdValidatePeerTask{
				region: d.region(),
				peer:   d.peer.Meta,
			},
		}
	}
}

func (d *peerMsgHandler) onReadyComputeHash(region *metapb.Region, index uint64, snap *engine.SnapAccess) {
	d.peer.ConsistencyState.LastCheckTime = time.Now()
	log.S().Infof("%s schedule compute hash task", d.tag())
	d.ctx.computeHashTaskSender <- task{
		tp: taskTypeComputeHash,
		data: &computeHashTask{
			region: region,
			index:  index,
			snap:   snap,
		},
	}
}

func (d *peerMsgHandler) onReadyVerifyHash(expectedIndex uint64, expectedHash []byte) {
	d.verifyAndStoreHash(expectedIndex, expectedHash)
}

func (d *peerMsgHandler) onHashComputed(index uint64, hash []byte) {
	if !d.verifyAndStoreHash(index, hash) {
		return
	}
	req := newVerifyHashRequest(d.regionID(), d.peer.Meta, d.peer.ConsistencyState)
	d.proposeRaftCommand(raftlog.NewRequest(req), nil)
}

/// Verify and store the hash to state. return true means the hash has been stored successfully.
func (d *peerMsgHandler) verifyAndStoreHash(expectedIndex uint64, expectedHash []byte) bool {
	state := d.peer.ConsistencyState
	index := state.Index
	if expectedIndex < index {
		log.S().Warnf("%s has scheduled a new hash, skip, index: %d, expected_index: %d, ",
			d.tag(), d.peer.ConsistencyState.Index, expectedIndex)
		return false
	}
	if expectedIndex == index {
		if len(state.Hash) == 0 {
			log.S().Warnf("%s duplicated consistency check detected, skip.", d.tag())
			return false
		}
		if !bytes.Equal(state.Hash, expectedHash) {
			panic(fmt.Sprintf("%s hash at %d not correct want %v, got %v",
				d.tag(), index, expectedHash, state.Hash))
		}
		log.S().Infof("%s consistency check pass, index %d", d.tag(), index)
		state.Hash = nil
		return false
	}
	if state.Index != 0 && len(state.Hash) > 0 {
		// Maybe computing is too slow or computed result is dropped due to channel full.
		// If computing is too slow, miss count will be increased twice.
		log.S().Warnf("%s hash belongs to wrong index, skip, index: %d, expected_index: %d",
			d.tag(), index, expectedIndex)
	}
	log.S().Infof("%s save hash for consistency check later, index: %d", d.tag(), index)
	state.Index = expectedIndex
	state.Hash = expectedHash
	return true
}

func maybeDestroySource(meta *storeMeta, targetID, sourceID uint64, epoch *metapb.RegionEpoch) bool {
	if mergeTargets, ok := meta.pendingMergeTargets[targetID]; ok {
		if targetEpoch, ok1 := mergeTargets[sourceID]; ok1 {
			log.S().Infof("[region %d] checking source %d epoch: %s, merge target epoch: %s",
				targetID, sourceID, epoch, targetEpoch)
			// The target peer will move on, namely, it will apply a snapshot generated after merge,
			// so destroy source peer.
			if epoch.Version > targetEpoch.Version {
				return true
			}
			// Wait till the target peer has caught up logs and source peer will be destroyed at that time.
			return false
		}
	}
	return false
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newVerifyHashRequest(regionID uint64, peer *metapb.Peer, state *ConsistencyState) *raft_cmdpb.RaftCmdRequest {
	request := newAdminRequest(regionID, peer)
	request.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_VerifyHash,
		VerifyHash: &raft_cmdpb.VerifyHashRequest{
			Index: state.Index,
			Hash:  state.Hash,
		},
	}
	return request
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

// Handle status commands here, separate the logic, maybe we can move it
// to another file later.
// Unlike other commands (write or admin), status commands only show current
// store status, so no need to handle it in raft group.
func (d *peerMsgHandler) executeStatusCommand(request *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, error) {
	cmdType := request.StatusRequest.CmdType
	var response *raft_cmdpb.StatusResponse
	switch cmdType {
	case raft_cmdpb.StatusCmdType_RegionLeader:
		response = d.executeRegionLeader()
	case raft_cmdpb.StatusCmdType_RegionDetail:
		var err error
		response, err = d.executeRegionDetail(request)
		if err != nil {
			return nil, err
		}
	case raft_cmdpb.StatusCmdType_InvalidStatus:
		return nil, errors.New("invalid status command!")
	}
	response.CmdType = cmdType

	resp := &raft_cmdpb.RaftCmdResponse{
		StatusResponse: response,
	}
	BindRespTerm(resp, d.peer.Term())
	return resp, nil // TODO: stub
}

func (d *peerMsgHandler) executeRegionLeader() *raft_cmdpb.StatusResponse {
	resp := &raft_cmdpb.StatusResponse{}
	if leader := d.peer.getPeerFromCache(d.peer.LeaderId()); leader != nil {
		resp.RegionLeader = &raft_cmdpb.RegionLeaderResponse{
			Leader: leader,
		}
	}
	return resp
}

func (d *peerMsgHandler) executeRegionDetail(request *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.StatusResponse, error) {
	if !d.peer.isInitialized() {
		regionID := request.Header.RegionId
		return nil, errors.Errorf("region %d not initialized", regionID)
	}
	resp := &raft_cmdpb.StatusResponse{
		RegionDetail: &raft_cmdpb.RegionDetailResponse{
			Region: d.region(),
		},
	}
	if leader := d.peer.getPeerFromCache(d.peer.LeaderId()); leader != nil {
		resp.RegionDetail = &raft_cmdpb.RegionDetailResponse{
			Leader: leader,
		}
	}
	return resp, nil
}

func (d *peerMsgHandler) onGenerateMetaChangeEvent(e *enginepb.ChangeSet) {
	log.S().Infof("region %d:%d generate meta change event", e.ShardID, e.ShardVer)
	region := d.region()
	header := raftlog.CustomHeader{
		RegionID: region.Id,
		Epoch:    raftlog.NewEpoch(region.RegionEpoch.Version, region.RegionEpoch.ConfVer),
		PeerID:   d.peer.Meta.Id,
		StoreID:  d.storeID(),
	}
	b := raftlog.NewBuilder(header)
	b.SetChangeSet(e)
	d.proposeRaftCommand(b.Build(), nil)
}

func (d *peerMsgHandler) updateFollowerSplitFilesDone(msg *rspb.RaftMessage) {
	if msg.ExtraMsg == nil || msg.ExtraMsg.Type != ExtraMessageTypeSplitFilesDone {
		return
	}
	fromPeer := msg.FromPeer.Id
	if fromPeer == d.peerID() {
		return
	}
	log.S().Infof("%d:%d follower %d split file done", d.regionID(), d.region().RegionEpoch.Version, fromPeer)
	d.peer.followersSplitFilesDone[fromPeer] = msg.RegionEpoch.Version
}

func (d *peerMsgHandler) onApplyChangeSetResult(result *MsgApplyChangeSetResult) {
	store := d.peer.Store()
	change := result.change
	d.hasReady = true
	if result.err != nil {
		log.S().Errorf("%d:%d failed to apply change set %s, err %v",
			result.change.ShardID, result.change.ShardVer, result.change, result.err)
		if result.change.Snapshot != nil {
			store.snapState = SnapState_ApplyAborted
		}
		return
	}
	if change.Snapshot != nil {
		log.S().Infof("%d on apply change set result", d.regionID())
		store.initialFlushed = true
		store.snapState = SnapState_Relax
		props := change.Snapshot.Properties
		if props != nil {
			store.stableApplyState = getApplyStateFromProps(props)
		}
	}
	if change.Flush != nil {
		store.initialFlushed = true
		props := change.Flush.Properties
		if props != nil {
			store.stableApplyState = getApplyStateFromProps(props)
		}
	}
	if change.SplitFiles != nil {
		d.ctx.applyMsgs.appendMsg(d.regionID(), NewPeerMsg(MsgTypeApplyResume, d.regionID(), nil))
	}
	if change.Stage > store.splitStage {
		log.S().Infof("%d:%d peer store split stage is changed from %s to %s",
			change.ShardID, change.ShardVer, store.splitStage, change.Stage)
		store.splitStage = change.Stage
	}
}
