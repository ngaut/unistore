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
	"github.com/pingcap/badger/protos"
	"go.uber.org/zap"
	"time"

	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/ngaut/unistore/tikv/raftstore/raftlog"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/codec"
	"github.com/uber-go/atomic"
)

type pendingCmd struct {
	index uint64
	term  uint64
	cb    *Callback
}

type pendingCmdQueue struct {
	normals    []pendingCmd
	confChange *pendingCmd
}

func (q *pendingCmdQueue) popNormal(term uint64) *pendingCmd {
	if len(q.normals) == 0 {
		return nil
	}
	cmd := &q.normals[0]
	if cmd.term > term {
		return nil
	}
	q.normals = q.normals[1:]
	return cmd
}

func (q *pendingCmdQueue) appendNormal(cmd pendingCmd) {
	q.normals = append(q.normals, cmd)
}

func (q *pendingCmdQueue) takeConfChange() *pendingCmd {
	// conf change will not be affected when changing between follower and leader,
	// so there is no need to check term.
	cmd := q.confChange
	q.confChange = nil
	return cmd
}

// TODO: seems we don't need to separate conf change from normal entries.
func (q *pendingCmdQueue) setConfChange(cmd *pendingCmd) {
	q.confChange = cmd
}

type changePeer struct {
	confChange *eraftpb.ConfChange
	peer       *metapb.Peer
	region     *metapb.Region
}

type keyRange struct {
	startKey []byte
	endKey   []byte
}

type apply struct {
	regionId uint64
	term     uint64
	entries  []eraftpb.Entry
}

type applyMetrics struct {
	sizeDiffHint   uint64
	deleteKeysHint uint64
	writtenBytes   uint64
	writtenKeys    uint64
}

type applyTaskRes struct {
	regionID    uint64
	applyState  applyState
	execResults []execResult
	metrics     applyMetrics
	merged      bool

	destroyPeerID uint64
}

type execResultChangePeer struct {
	cp changePeer
}

type execResultCompactLog struct {
	truncatedIndex uint64
	firstIndex     uint64
}

type execResultSplitRegion struct {
	regions []*metapb.Region
	derived *metapb.Region
}

type execResultPrepareMerge struct {
	region *metapb.Region
	state  *rspb.MergeState
}

type execResultCommitMerge struct {
	region *metapb.Region
	source *metapb.Region
}

type execResultRollbackMerge struct {
	region *metapb.Region
	commit uint64
}

type execResultComputeHash struct {
	region *metapb.Region
	index  uint64
	snap   *mvcc.DBSnapshot
}

type execResultVerifyHash struct {
	index uint64
	hash  []byte
}

type execResultDeleteRange struct {
	ranges []keyRange
}

type execResult = interface{}

type applyResultType int

const (
	applyResultTypeNone              applyResultType = 0
	applyResultTypeExecResult        applyResultType = 1
	applyResultTypeWaitMergeResource applyResultType = 2
)

type applyResult struct {
	tp   applyResultType
	data interface{}
}

type applyExecContext struct {
	index      uint64
	term       uint64
	applyState applyState
}

type applyCallback struct {
	region *metapb.Region
	cbs    []*Callback
}

func (c *applyCallback) invokeAll(doneApplyTime time.Time) {
	for _, cb := range c.cbs {
		if cb != nil {
			cb.applyDoneTime = doneApplyTime
			cb.wg.Done()
		}
	}
}

func (c *applyCallback) push(cb *Callback, resp *raft_cmdpb.RaftCmdResponse) {
	if cb != nil {
		cb.resp = resp
	}
	c.cbs = append(c.cbs, cb)
}

type proposal struct {
	isConfChange bool
	index        uint64
	term         uint64
	cb           *Callback
}

type regionProposal struct {
	Id       uint64
	RegionId uint64
	Props    []*proposal
}

func newRegionProposal(id uint64, regionId uint64, props []*proposal) *regionProposal {
	return &regionProposal{
		Id:       id,
		RegionId: regionId,
		Props:    props,
	}
}

type registration struct {
	id         uint64
	term       uint64
	applyState applyState
	region     *metapb.Region
}

func newRegistration(peer *Peer) *registration {
	return &registration{
		id:         peer.PeerId(),
		term:       peer.Term(),
		applyState: peer.Store().applyState,
		region:     peer.Region(),
	}
}

type applyMsgs struct {
	msgs []Msg
}

func (r *applyMsgs) appendMsg(regionID uint64, msg Msg) {
	msg.RegionID = regionID
	r.msgs = append(r.msgs, msg)
	return
}

type applyContext struct {
	tag              string
	timer            *time.Time
	regionScheduler  chan<- task
	applyResCh       chan<- Msg
	engines          *Engines
	applyBatch       *applyBatch
	cbs              []applyCallback
	applyTaskResList []*applyTaskRes
	execCtx          *applyExecContext
	wb               *KVWriteBatch
	lastAppliedIndex uint64
	committedCount   int

	// Indicates that WAL can be synchronized when data is written to KV engine.
	enableSyncLog bool
	// Whether to use the delete range API instead of deleting one by one.
	useDeleteRange bool
}

func newApplyContext(tag string, regionScheduler chan<- task, engines *Engines,
	applyResCh chan<- Msg, cfg *Config) *applyContext {
	return &applyContext{
		tag:             tag,
		regionScheduler: regionScheduler,
		engines:         engines,
		applyResCh:      applyResCh,
		enableSyncLog:   cfg.SyncLog,
		useDeleteRange:  cfg.UseDeleteRange,
		wb:              NewKVWriteBatch(engines.kv),
	}
}

/// Prepares for applying entries for `applier`.
///
/// A general apply progress for an applier is:
/// `prepare_for` -> `commit` [-> `commit` ...] -> `finish_for`.
/// After all appliers are handled, `write_to_db` method should be called.
func (ac *applyContext) prepareFor(d *applier) {
	ac.cbs = append(ac.cbs, applyCallback{region: d.region})
	ac.lastAppliedIndex = d.applyState.appliedIndex
}

const applyStateKey = "applyState"

/// Commits all changes have done for applier. `persistent` indicates whether
/// write the changes into rocksdb.
///
/// This call is valid only when it's between a `prepare_for` and `finish_for`.
func (ac *applyContext) commit(d *applier) {
	if ac.lastAppliedIndex < d.applyState.appliedIndex {
		ac.wb.SetApplyState(d.region.Id, d.applyState)
	}
	// last_applied_index doesn't need to be updated, set persistent to true will
	// force it call `prepare_for` automatically.
	ac.commitOpt(d, true)
}

func (ac *applyContext) commitOpt(d *applier, persistent bool) {
	d.updateMetrics(ac)
	if persistent {
		ac.writeToDB()
		ac.prepareFor(d)
	}
}

/// Writes all the changes into badger.
func (ac *applyContext) writeToDB() {
	err := ac.wb.WriteToEngine()
	y.Assert(err == nil)
	ac.wb.Reset()
	doneApply := time.Now()
	for _, cb := range ac.cbs {
		cb.invokeAll(doneApply)
	}
	ac.cbs = make([]applyCallback, 0, cap(ac.cbs))
}

/// Finishes `Apply`s for the applier.
func (ac *applyContext) finishFor(d *applier, results []execResult) {
	if !d.pendingRemove {
		ac.wb.SetApplyState(d.region.Id, d.applyState)
	}
	ac.commitOpt(d, false)
	res := &applyTaskRes{
		regionID:    d.region.Id,
		applyState:  d.applyState,
		execResults: results,
		metrics:     d.metrics,
	}
	ac.applyTaskResList = append(ac.applyTaskResList, res)
}

func (ac *applyContext) flush() {
	// TODO: this check is too hacky, need to be more verbose and less buggy.
	t := ac.timer
	ac.timer = nil
	if t == nil {
		return
	}
	// Write to engine
	// raftsotre.sync-log = true means we need prevent data loss when power failure.
	// take raft log gc for example, we write kv WAL first, then write raft WAL,
	// if power failure happen, raft WAL may synced to disk, but kv WAL may not.
	// so we use sync-log flag here.
	ac.writeToDB()
	if len(ac.applyTaskResList) > 0 {
		for i, res := range ac.applyTaskResList {
			ac.applyResCh <- NewPeerMsg(MsgTypeApplyRes, res.regionID, res)
			ac.applyTaskResList[i] = nil
		}
		ac.applyTaskResList = ac.applyTaskResList[:0]
	}
	ac.committedCount = 0
}

/// Calls the callback of `cmd` when the Region is removed.
func notifyRegionRemoved(regionID, peerID uint64, cmd pendingCmd) {
	log.S().Debugf("region %d is removed, peerID %d, index %d, term %d", regionID, peerID, cmd.index, cmd.term)
	notifyReqRegionRemoved(regionID, cmd.cb)
}

func notifyReqRegionRemoved(regionID uint64, cb *Callback) {
	cb.Done(ErrRespRegionNotFound(regionID))
}

/// Calls the callback of `cmd` when it can not be processed further.
func notifyStaleCommand(regionID, peerID, term uint64, cmd pendingCmd) {
	log.S().Infof("command is stale, skip. regionID %d, peerID %d, index %d, term %d",
		regionID, peerID, cmd.index, cmd.term)
	notifyStaleReq(term, cmd.cb)
}

func notifyStaleReq(term uint64, cb *Callback) {
	cb.Done(ErrRespStaleCommand(term))
}

/// Checks if a write is needed to be issued before handling the command.
func shouldWriteToEngine(rlog raftlog.RaftLog) bool {
	cmd := rlog.GetRaftCmdRequest()
	if cmd == nil {
		return false
	}
	if cmd.AdminRequest != nil {
		switch cmd.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_ComputeHash, // ComputeHash require an up to date snapshot.
			raft_cmdpb.AdminCmdType_CommitMerge, // Merge needs to get the latest apply index.
			raft_cmdpb.AdminCmdType_RollbackMerge:
			return true
		}
	}

	// Some commands may modify keys covered by the current write batch, so we
	// must write the current write batch to the engine first.
	for _, req := range cmd.Requests {
		if req.DeleteRange != nil {
			return true
		}
		if req.IngestSst != nil {
			return true
		}
	}
	return false
}

/// A struct that stores the state related to Merge.
///
/// When executing a `CommitMerge`, the source peer may have not applied
/// to the required index, so the target peer has to abort current execution
/// and wait for it asynchronously.
///
/// When rolling the stack, all states required to recover are stored in
/// this struct.
/// TODO: check whether generator/coroutine is a good choice in this case.
type waitSourceMergeState struct {
	/// All of the entries that need to continue to be applied after
	/// the source peer has applied its logs.
	pendingEntries []eraftpb.Entry
	/// All of messages that need to continue to be handled after
	/// the source peer has applied its logs and pending entries
	/// are all handled.
	pendingMsgs []Msg
	/// A flag that indicates whether the source peer has applied to the required
	/// index. If the source peer is ready, this flag should be set to the region id
	/// of source peer.
	readyToMerge *atomic.Uint64
	/// When handling `CatchUpLogs` message, maybe there is a merge cascade, namely,
	/// a source peer to catch up logs whereas the logs contain a `CommitMerge`.
	/// In this case, the source peer needs to merge another source peer first, so storing the
	/// `CatchUpLogs` message in this field, and once the cascaded merge and all other pending
	/// msgs are handled, the source peer will check this field and then send `LogsUpToDate`
	/// message to its target peer.
	catchUpLogs *catchUpLogs
}

func (s *waitSourceMergeState) String() string {
	return fmt.Sprintf("waitSourceMergeState{pending_entries:%d, pending_msgs:%d, ready_to_merge:%d, catch_up_logs:%v}",
		len(s.pendingEntries), len(s.pendingMsgs), s.readyToMerge.Load(), s.catchUpLogs != nil)
}

/// The applier of a Region which is responsible for handling committed
/// raft log entries of a Region.
///
/// `Apply` is a term of Raft, which means executing the actual commands.
/// In Raft, once some log entries are committed, for every peer of the Raft
/// group will apply the logs one by one. For write commands, it does write or
/// delete to local engine; for admin commands, it does some meta change of the
/// Raft group.
///
/// The raft worker receives all the apply tasks of different Regions
/// located at this store, and it will get the corresponding applier to
/// handle the apply task to make the code logic more clear.
type applier struct {
	id     uint64
	term   uint64
	region *metapb.Region
	tag    string

	/// If the applier should be stopped from polling.
	/// A applier can be stopped in conf change, merge or requested by destroy message.
	stopped bool
	/// Set to true when removing itself because of `ConfChangeType::RemoveNode`, and then
	/// any following committed logs in same Ready should be applied failed.
	pendingRemove bool

	/// The commands waiting to be committed and applied
	pendingCmds pendingCmdQueue

	/// Marks the applier as merged by CommitMerge.
	merged bool

	/// Indicates the peer is in merging, if that compact log won't be performed.
	isMerging bool
	/// Records the epoch version after the last merge.
	lastMergeVersion uint64
	/// A temporary state that keeps track of the progress of the source peer state when
	/// CommitMerge is unable to be executed.
	waitMergeState *waitSourceMergeState
	// ID of last region that reports ready.
	readySourceRegion uint64

	/// We writes apply_state to KV DB, in one write batch together with kv data.
	///
	/// If we write it to Raft DB, apply_state and kv data (Put, Delete) are in
	/// separate WAL file. When power failure, for current raft log, apply_index may synced
	/// to file, but KV data may not synced to file, so we will lose data.
	applyState applyState

	// redoIdx is the raft log index starts redo for lockStore.
	redoIndex uint64

	/// The local metrics, and it will be flushed periodically.
	metrics applyMetrics
}

func newApplier(reg *registration) *applier {
	return &applier{
		id:         reg.id,
		tag:        fmt.Sprintf("[region %d] %d", reg.region.Id, reg.id),
		region:     reg.region,
		applyState: reg.applyState,
		term:       reg.term,
	}
}

/// Handles all the committed_entries, namely, applies the committed entries.
func (a *applier) handleRaftCommittedEntries(aCtx *applyContext, committedEntries []eraftpb.Entry) {
	if len(committedEntries) == 0 {
		return
	}
	aCtx.prepareFor(a)
	aCtx.committedCount += len(committedEntries)
	// If we send multiple ConfChange commands, only first one will be proposed correctly,
	// others will be saved as a normal entry with no data, so we must re-propose these
	// commands again.
	aCtx.committedCount += len(committedEntries)
	var results []execResult
	for i := range committedEntries {
		entry := &committedEntries[i]
		if a.pendingRemove {
			// This peer is about to be destroyed, skip everything.
			break
		}
		expectedIndex := a.applyState.appliedIndex + 1
		if expectedIndex != entry.Index {
			// Msg::CatchUpLogs may have arrived before Msg::Apply.
			if expectedIndex > entry.GetIndex() && a.isMerging {
				log.S().Infof("skip log as it's already applied. region_id %d, peer_id %d, index %d",
					a.region.Id, a.id, entry.Index)
				continue
			}
			panic(fmt.Sprintf("%s expect index %d, but got %d", a.tag, expectedIndex, entry.Index))
		}
		var res applyResult
		switch entry.EntryType {
		case eraftpb.EntryType_EntryNormal:
			res = a.handleRaftEntryNormal(aCtx, entry)
		case eraftpb.EntryType_EntryConfChange:
			res = a.handleRaftEntryConfChange(aCtx, entry)
		}
		switch res.tp {
		case applyResultTypeNone:
		case applyResultTypeExecResult:
			results = append(results, res.data)
		case applyResultTypeWaitMergeResource:
			readyToMerge := res.data.(*atomic.Uint64)
			aCtx.committedCount -= len(committedEntries) - i
			pendingEntries := make([]eraftpb.Entry, 0, len(committedEntries)-i)
			// Note that CommitMerge is skipped when `WaitMergeSource` is returned.
			// So we need to enqueue it again and execute it again when resuming.
			pendingEntries = append(pendingEntries, committedEntries[i:]...)
			aCtx.finishFor(a, results)
			a.waitMergeState = &waitSourceMergeState{
				pendingEntries: pendingEntries,
				readyToMerge:   readyToMerge,
			}
			return
		}
	}
	aCtx.finishFor(a, results)
}

func (a *applier) updateMetrics(aCtx *applyContext) {
	for _, wb := range aCtx.wb.batches {
		a.metrics.writtenBytes += uint64(wb.EstimatedSize())
		a.metrics.writtenKeys += uint64(wb.NumEntries())
	}
}

func (a *applier) handleRaftEntryNormal(aCtx *applyContext, entry *eraftpb.Entry) applyResult {
	index := entry.Index
	term := entry.Term
	if len(entry.Data) > 0 {
		rlog := raftlog.DecodeLog(entry)
		if shouldWriteToEngine(rlog) {
			aCtx.commit(a)
		}
		return a.processRaftCmd(aCtx, index, term, rlog)
	}

	// when a peer become leader, it will send an empty entry.
	a.applyState.appliedIndex = index
	a.applyState.appliedIndexTerm = term
	y.Assert(term > 0)
	for {
		cmd := a.pendingCmds.popNormal(term - 1)
		if cmd == nil {
			break
		}
		// apparently, all the callbacks whose term is less than entry's term are stale.
		cb := &aCtx.cbs[len(aCtx.cbs)-1]
		cmd.cb.resp = ErrRespStaleCommand(term)
		cb.cbs = append(cb.cbs, cmd.cb)
	}
	return applyResult{}
}

func (a *applier) handleRaftEntryConfChange(aCtx *applyContext, entry *eraftpb.Entry) applyResult {
	index := entry.Index
	term := entry.Term
	confChange := new(eraftpb.ConfChange)
	if err := confChange.Unmarshal(entry.Data); err != nil {
		panic(err)
	}
	cmd := new(raft_cmdpb.RaftCmdRequest)
	if err := cmd.Unmarshal(confChange.Context); err != nil {
		panic(err)
	}
	result := a.processRaftCmd(aCtx, index, term, raftlog.NewRequest(cmd))
	switch result.tp {
	case applyResultTypeNone:
		// If failed, tell Raft that the `ConfChange` was aborted.
		return applyResult{tp: applyResultTypeExecResult, data: &execResultChangePeer{}}
	case applyResultTypeExecResult:
		cp := result.data.(*execResultChangePeer)
		cp.cp.confChange = confChange
		return applyResult{tp: applyResultTypeExecResult, data: result.data}
	default:
		panic("unreachable")
	}
}

func (a *applier) findCallback(index, term uint64, isConfChange bool) *Callback {
	regionID := a.region.Id
	peerID := a.id
	if isConfChange {
		cmd := a.pendingCmds.takeConfChange()
		if cmd == nil {
			return nil
		}
		if cmd.index == index && cmd.term == term {
			return cmd.cb
		}
		notifyStaleCommand(regionID, peerID, term, *cmd)
		return nil
	}
	for {
		head := a.pendingCmds.popNormal(term)
		if head == nil {
			break
		}
		if head.index == index && head.term == term {
			return head.cb
		}
		// Because of the lack of original RaftCmdRequest, we skip calling
		// coprocessor here.
		notifyStaleCommand(regionID, peerID, term, *head)
	}
	return nil
}

func (a *applier) processRaftCmd(aCtx *applyContext, index, term uint64, rlog raftlog.RaftLog) applyResult {
	if index == 0 {
		panic(fmt.Sprintf("%s process raft cmd need a none zero index", a.tag))
	}
	isConfChange := GetChangePeerCmd(rlog.GetRaftCmdRequest()) != nil
	resp, result := a.applyRaftCmd(aCtx, index, term, rlog)
	if result.tp == applyResultTypeWaitMergeResource {
		return result
	}
	log.S().Debugf("applied command. region_id %d, peer_id %d, index %d", a.region.Id, a.id, index)

	// TODO: if we have exec_result, maybe we should return this callback too. Outer
	// store will call it after handing exec result.
	BindRespTerm(resp, term)
	cmdCB := a.findCallback(index, term, isConfChange)
	aCtx.cbs[len(aCtx.cbs)-1].push(cmdCB, resp)
	return result
}

/// Applies raft command.
///
/// An apply operation can fail in the following situations:
///   1. it encounters an error that will occur on all stores, it can continue
/// applying next entry safely, like epoch not match for example;
///   2. it encounters an error that may not occur on all stores, in this case
/// we should try to apply the entry again or panic. Considering that this
/// usually due to disk operation fail, which is rare, so just panic is ok.
func (a *applier) applyRaftCmd(aCtx *applyContext, index, term uint64,
	rlog raftlog.RaftLog) (*raft_cmdpb.RaftCmdResponse, applyResult) {
	// if pending remove, apply should be aborted already.
	y.Assert(!a.pendingRemove)

	aCtx.execCtx = a.newCtx(index, term)
	// aCtx.wb.SetSafePoint()
	resp, applyResult, err := a.execRaftCmd(aCtx, rlog)
	if err != nil {
		// TODO: clear dirty values.
		// aCtx.wb.RollbackToSafePoint()
		if _, ok := err.(*ErrEpochNotMatch); ok {
			log.S().Debugf("epoch not match region_id %d, peer_id %d, err %v", a.region.Id, a.id, err)
		} else {
			log.S().Errorf("execute raft command region_id %d, peer_id %d, err %v", a.region.Id, a.id, err)
		}
		resp = ErrResp(err)
	}
	if applyResult.tp == applyResultTypeWaitMergeResource {
		return resp, applyResult
	}
	a.applyState = aCtx.execCtx.applyState
	aCtx.execCtx = nil
	a.applyState.appliedIndex = index
	a.applyState.appliedIndexTerm = term

	if applyResult.tp == applyResultTypeExecResult {
		switch x := applyResult.data.(type) {
		case *execResultChangePeer:
			a.region = x.cp.region
		case *execResultSplitRegion:
			a.region = x.derived
			a.metrics.sizeDiffHint = 0
			a.metrics.deleteKeysHint = 0
		case *execResultPrepareMerge:
			a.region = x.region
			a.isMerging = true
		case *execResultCommitMerge:
			a.region = x.region
			a.lastMergeVersion = x.region.RegionEpoch.Version
		case *execResultRollbackMerge:
			a.region = x.region
			a.isMerging = false
		default:
		}
	}
	return resp, applyResult
}

func (a *applier) clearAllCommandsAsStale() {
	for i, cmd := range a.pendingCmds.normals {
		notifyStaleCommand(a.region.Id, a.id, a.term, cmd)
		a.pendingCmds.normals[i] = pendingCmd{}
	}
	a.pendingCmds.normals = a.pendingCmds.normals[:0]
	if cmd := a.pendingCmds.takeConfChange(); cmd != nil {
		notifyStaleCommand(a.region.Id, a.id, a.term, *cmd)
	}
}

func (a *applier) newCtx(index, term uint64) *applyExecContext {
	return &applyExecContext{
		index:      index,
		term:       term,
		applyState: a.applyState,
	}
}

// Only errors that will also occur on all other stores should be returned.
func (a *applier) execRaftCmd(aCtx *applyContext, rlog raftlog.RaftLog) (
	resp *raft_cmdpb.RaftCmdResponse, result applyResult, err error) {
	// Include region for epoch not match after merge may cause key not in range.
	includeRegion := rlog.Epoch().Ver() >= a.lastMergeVersion
	err = checkRegionEpoch(rlog, a.region, includeRegion)
	if err != nil {
		return
	}
	req := rlog.GetRaftCmdRequest()
	if req.GetAdminRequest() != nil {
		return a.execAdminCmd(aCtx, req)
	}
	resp, result = a.execWriteCmd(aCtx, rlog)
	return
}

func (a *applier) execAdminCmd(aCtx *applyContext, req *raft_cmdpb.RaftCmdRequest) (
	resp *raft_cmdpb.RaftCmdResponse, result applyResult, err error) {
	adminReq := req.AdminRequest
	cmdType := adminReq.CmdType
	if cmdType != raft_cmdpb.AdminCmdType_CompactLog && cmdType != raft_cmdpb.AdminCmdType_CommitMerge {
		log.S().Infof("%s execute admin command. term %d, index %d, command %s",
			a.tag, aCtx.execCtx.term, aCtx.execCtx.index, adminReq)
	}
	var adminResp *raft_cmdpb.AdminResponse
	switch cmdType {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		adminResp, result, err = a.execChangePeer(adminReq)
	case raft_cmdpb.AdminCmdType_Split:
		adminResp, result, err = a.execSplit(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_BatchSplit:
		adminResp, result, err = a.execBatchSplit(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_CompactLog:
		adminResp, result, err = a.execCompactLog(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		err = errors.New("transfer leader won't execute")
	case raft_cmdpb.AdminCmdType_ComputeHash:
		adminResp, result, err = a.execComputeHash(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_VerifyHash:
		adminResp, result, err = a.execVerifyHash(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_PrepareMerge:
		adminResp, result, err = a.execPrepareMerge(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_CommitMerge:
		adminResp, result, err = a.execCommitMerge(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_RollbackMerge:
		adminResp, result, err = a.execRollbackMerge(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
		err = errors.New("unsupported command type")
	}
	if err != nil {
		return
	}
	adminResp.CmdType = cmdType
	resp = newCmdRespForReq(req)
	resp.AdminResponse = adminResp
	return
}

func (a *applier) execWriteCmd(aCtx *applyContext, rlog raftlog.RaftLog) (
	resp *raft_cmdpb.RaftCmdResponse, result applyResult) {
	if cl, ok := rlog.(*raftlog.CustomRaftLog); ok {
		cnt := a.execCustomLog(aCtx, cl)
		resp = &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
		resp.Responses = make([]*raft_cmdpb.Response, cnt)
		return
	}
	req := rlog.GetRaftCmdRequest()
	requests := req.GetRequests()
	y.Assert(len(requests) == 1)
	delRange := requests[0].DeleteRange
	y.Assert(delRange != nil)
	a.execDeleteRange(aCtx, delRange)
	delRangeResp := &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_DeleteRange}
	resp = newCmdRespForReq(req)
	resp.Responses = []*raft_cmdpb.Response{delRangeResp}
	result = applyResult{
		tp:   applyResultTypeExecResult,
		data: &execResultDeleteRange{},
	}
	return
}

func (a *applier) isFollower(rlog raftlog.RaftLog) bool {
	return a.id != rlog.PeerID()
}

func (a *applier) execCustomLog(aCtx *applyContext, cl *raftlog.CustomRaftLog) int {
	var cnt int
	switch cl.Type() {
	case raftlog.TypePrewrite, raftlog.TypePessimisticLock:
		cl.IterateLock(func(key, val []byte) {
			aCtx.wb.SetLock(cl.RegionID(), key, val)
			cnt++
		})
	case raftlog.TypeCommit:
		cl.IterateCommit(func(key, val []byte, commitTS uint64) {
			a.commitLock(aCtx, cl.RegionID(), key, val, commitTS)
			cnt++
		})
	case raftlog.TypeRollback:
		cl.IterateRollback(func(key []byte, startTS uint64, deleteLock bool) {
			aCtx.wb.Rollback(cl.RegionID(), y.KeyWithTs(key, startTS))
			if deleteLock {
				aCtx.wb.DeleteLock(cl.RegionID(), key)
			}
			cnt++
		})
	case raftlog.TypePessimisticRollback:
		cl.IterateKeysOnly(func(key []byte) {
			aCtx.wb.DeleteLock(cl.RegionID(), key)
			cnt++
		})
	case raftlog.TypePreSplit:
		var splitKeys [][]byte
		cl.IterateKeysOnly(func(key []byte) {
			splitKeys = append(splitKeys, key)
		})
		err := aCtx.engines.kv.PreSplit(a.region.Id, a.region.RegionEpoch.Version, splitKeys)
		y.AssertTruef(err == nil, "preSplit error %v", err)
	case raftlog.TypeChangeSet:
		changeSet, err := cl.GetShardChangeSet()
		y.Assert(err == nil)
		a.executeChangeSet(aCtx, changeSet, a.isFollower(cl))
	}
	return cnt
}

func (a *applier) executeChangeSet(aCtx *applyContext, changeSet *protos.ShardChangeSet, isFollower bool) {
	if isFollower {
		y.Assert(changeSet.Flush != nil || changeSet.SplitFiles != nil || changeSet.Compaction != nil)
		aCtx.regionScheduler <- task{tp: taskTypeRegionFollowerChangeSet, data: changeSet}
	}
}

func (a *applier) commitLock(aCtx *applyContext, regionID uint64, rawKey []byte, val []byte, commitTS uint64) {
	lock := mvcc.DecodeLock(val)
	var sizeDiff int64
	userMeta := mvcc.NewDBUserMeta(lock.StartTS, commitTS)
	if lock.Op != uint8(kvrpcpb.Op_Lock) {
		aCtx.wb.SetWithUserMeta(regionID, y.KeyWithTs(rawKey, commitTS), lock.Value, userMeta)
		sizeDiff = int64(len(rawKey) + len(lock.Value))
	} else if bytes.Equal(lock.Primary, rawKey) {
		aCtx.wb.SetOpLock(regionID, y.KeyWithTs(rawKey, commitTS), userMeta)
	}
	if sizeDiff > 0 {
		a.metrics.sizeDiffHint += uint64(sizeDiff)
	}
	aCtx.wb.DeleteLock(regionID, rawKey)
}

func (a *applier) execDeleteRange(aCtx *applyContext, req *raft_cmdpb.DeleteRangeRequest) {
	_, startKey, err := codec.DecodeBytes(req.StartKey, nil)
	if err != nil {
		panic(req.StartKey)
	}
	_, endKey, err := codec.DecodeBytes(req.EndKey, nil)
	if err != nil {
		panic(req.EndKey)
	}

	if bytes.Equal(RawStartKey(a.region), startKey) && bytes.Equal(RawEndKey(a.region), endKey) {
		err = aCtx.engines.kv.RemoveShard(a.region.Id, true)
		if err != nil {
			log.Error("execDeleteRange failed", zap.Error(err))
		}
	} else {
		// TODO: implement
		log.Error("execDeleteRange not implemented yet")
	}
	return
}

func (a *applier) execChangePeer(req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	request := req.ChangePeer
	peer := request.Peer
	storeID := peer.StoreId
	changeType := request.ChangeType
	region := new(metapb.Region)
	err = CloneMsg(a.region, region)
	if err != nil {
		return
	}
	log.S().Infof("%s exec ConfChange, peer_id %d, type %s, epoch %s",
		a.tag, peer.Id, changeType, region.RegionEpoch)

	// TODO: we should need more check, like peer validation, duplicated id, etc.
	region.RegionEpoch.ConfVer++

	switch changeType {
	case eraftpb.ConfChangeType_AddNode:
		var exist bool
		if p := findPeer(region, storeID); p != nil {
			exist = true
			if !(p.Role == metapb.PeerRole_Learner) || p.Id != peer.Id {
				errMsg := fmt.Sprintf("%s can't add duplicated peer, peer %s, region %s",
					a.tag, p, a.region)
				log.S().Error(errMsg)
				err = errors.New(errMsg)
				return
			}
			p.Role = metapb.PeerRole_Voter
		}
		if !exist {
			// TODO: Do we allow adding peer in same node?
			region.Peers = append(region.Peers, peer)
		}
		log.S().Infof("%s add peer successfully, peer %s, region %s", a.tag, peer, a.region)
	case eraftpb.ConfChangeType_RemoveNode:
		if p := removePeer(region, storeID); p != nil {
			if !PeerEqual(p, peer) {
				errMsg := fmt.Sprintf("%s ignore remove unmatched peer, expected_peer %s, got_peer %s",
					a.tag, peer, p)
				log.S().Error(errMsg)
				err = errors.New(errMsg)
				return
			}
			if a.id == peer.Id {
				// Remove ourself, we will destroy all region data later.
				// So we need not to apply following logs.
				a.stopped = true
				a.pendingRemove = true
			}
		} else {
			errMsg := fmt.Sprintf("%s removing missing peers, peer %s, region %s",
				a.tag, peer, a.region)
			log.S().Error(errMsg)
			err = errors.New(errMsg)
			return
		}
		log.S().Infof("%s remove peer successfully, peer %s, region %s", a.tag, peer, a.region)
	case eraftpb.ConfChangeType_AddLearnerNode:
		if findPeer(region, storeID) != nil {
			errMsg := fmt.Sprintf("%s can't add duplicated learner, peer %s, region %s",
				a.tag, peer, a.region)
			log.S().Error(errMsg)
			err = errors.New(errMsg)
			return
		}
		region.Peers = append(region.Peers, peer)
		log.S().Infof("%s add learner successfully, peer %s, region %s", a.tag, peer, a.region)
	}
	resp = &raft_cmdpb.AdminResponse{
		ChangePeer: &raft_cmdpb.ChangePeerResponse{
			Region: region,
		},
	}
	result = applyResult{
		tp: applyResultTypeExecResult,
		data: &execResultChangePeer{
			cp: changePeer{
				confChange: new(eraftpb.ConfChange),
				region:     region,
				peer:       peer,
			},
		},
	}
	return
}

func (a *applier) execSplit(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	split := req.Split
	adminReq := &raft_cmdpb.AdminRequest{
		Splits: &raft_cmdpb.BatchSplitRequest{
			Requests:    []*raft_cmdpb.SplitRequest{split},
			RightDerive: split.RightDerive,
		},
	}
	// This method is executed only when there are unapplied entries after being restarted.
	// So there will be no callback, it's OK to return a response that does not matched
	// with its request.
	return a.execBatchSplit(aCtx, adminReq)
}

func (a *applier) execBatchSplit(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	var derived *metapb.Region
	var regions []*metapb.Region
	derived, regions, err = a.splitGenNewRegionMetas(req.Splits)
	if err != nil {
		return
	}
	a.applyState.appliedIndex = aCtx.execCtx.index
	newShardProps := make([]*protos.ShardProperties, len(regions))
	for i := 0; i < len(regions); i++ {
		props := new(protos.ShardProperties)
		props.ShardID = regions[i].Id
		props.Keys = append(props.Keys, applyStateKey)
		if regions[i].Id == a.region.Id {
			props.Values = append(props.Values, a.applyState.Marshal())
		} else {
			props.Values = append(props.Values, newInitialApplyState().Marshal())
		}
		newShardProps[i] = props
	}
	_, err = aCtx.engines.kv.FinishSplit(a.region.Id, a.region.RegionEpoch.Version, newShardProps)
	if err != nil {
		return
	}
	resp = &raft_cmdpb.AdminResponse{
		Splits: &raft_cmdpb.BatchSplitResponse{
			Regions: regions,
		},
	}
	result = applyResult{tp: applyResultTypeExecResult, data: &execResultSplitRegion{
		regions: regions,
		derived: derived,
	}}
	return
}

func (a *applier) splitGenNewRegionMetas(splitReqs *raft_cmdpb.BatchSplitRequest) (derived *metapb.Region, regions []*metapb.Region, err error) {
	if len(splitReqs.Requests) == 0 {
		return nil, nil, errors.New("missing split key")
	}
	derived = new(metapb.Region)
	if err := CloneMsg(a.region, derived); err != nil {
		panic(err)
	}
	rightDerive := splitReqs.RightDerive
	newRegionCnt := len(splitReqs.Requests)
	regions = make([]*metapb.Region, 0, newRegionCnt+1)
	keys := make([][]byte, 0, newRegionCnt+1)
	keys = append(keys, derived.StartKey)
	for _, request := range splitReqs.Requests {
		splitKey := request.SplitKey
		if len(splitKey) == 0 {
			return nil, nil, errors.New("missing split key")
		}
		if bytes.Compare(splitKey, keys[len(keys)-1]) <= 0 {
			return nil, nil, errors.Errorf("invalid split request:%s", splitReqs)
		}
		if len(request.NewPeerIds) != len(derived.Peers) {
			return nil, nil, errors.Errorf("invalid new peer id count, need %d but got %d",
				len(derived.Peers), len(request.NewPeerIds))
		}
		keys = append(keys, splitKey)
	}
	keys = append(keys, derived.EndKey)
	err = CheckKeyInRegion(keys[len(keys)-2], a.region)
	if err != nil {
		return nil, nil, err
	}
	log.S().Infof("%s split region %s, keys %v", a.tag, a.region, keys)
	derived.RegionEpoch.Version += uint64(newRegionCnt)
	// Note that the split requests only contain ids for new regions, so we need
	// to handle new regions and old region separately.
	if !rightDerive {
		derived.EndKey = keys[1]
		keys = keys[1:]
		regions = append(regions, derived)
	}
	for i, request := range splitReqs.Requests {
		newRegion := &metapb.Region{
			Id:          request.NewRegionId,
			RegionEpoch: derived.RegionEpoch,
			StartKey:    keys[i],
			EndKey:      keys[i+1],
		}
		newRegion.Peers = make([]*metapb.Peer, len(derived.Peers))
		for j := range newRegion.Peers {
			newRegion.Peers[j] = &metapb.Peer{
				Id:      request.NewPeerIds[j],
				StoreId: derived.Peers[j].StoreId,
				Role:    derived.Peers[j].Role,
			}
		}
		regions = append(regions, newRegion)
	}
	if rightDerive {
		derived.StartKey = keys[len(keys)-2]
		regions = append(regions, derived)
	}
	return derived, regions, nil
}

func (a *applier) execPrepareMerge(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	return // TODO: merge
}

func (a *applier) execCommitMerge(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	return // TODO: merge
}

func (a *applier) execRollbackMerge(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	return // TODO: merge
}

func (a *applier) execCompactLog(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	compactIndex := req.CompactLog.CompactIndex
	resp = new(raft_cmdpb.AdminResponse)
	applyState := &aCtx.execCtx.applyState
	firstIndex := firstIndex(*applyState)
	if compactIndex <= firstIndex {
		log.S().Debugf("%s compact index <= first index, no need to compact", a.tag)
		return
	}
	if a.isMerging {
		log.S().Debugf("%s in merging mode, skip compact", a.tag)
		return
	}
	compactTerm := req.CompactLog.CompactTerm
	if compactTerm == 0 {
		log.S().Infof("%s compact term missing, skip", a.tag)
		// old format compact log command, safe to ignore.
		err = errors.New("command format is outdated, please upgrade leader")
		return
	}

	// compact failure is safe to be omitted, no need to assert.
	err = CompactRaftLog(a.tag, applyState, compactIndex, compactTerm)
	if err != nil {
		return
	}
	result = applyResult{tp: applyResultTypeExecResult, data: &execResultCompactLog{
		truncatedIndex: applyState.truncatedIndex,
		firstIndex:     firstIndex,
	}}
	return
}

func (a *applier) execComputeHash(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	resp = new(raft_cmdpb.AdminResponse)
	// TODO: run in goroutine.
	return
}

func (a *applier) execVerifyHash(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	verifyReq := req.VerifyHash
	resp = new(raft_cmdpb.AdminResponse)
	result = applyResult{tp: applyResultTypeExecResult, data: &execResultVerifyHash{
		index: verifyReq.Index,
		hash:  verifyReq.Hash,
	}}
	return
}

type catchUpLogs struct {
	merge        *raft_cmdpb.CommitMergeRequest
	readyToMerge *atomic.Uint64
}

func newApplierFromPeer(peer *peerFsm) *applier {
	reg := newRegistration(peer.peer)
	return newApplier(reg)
}

/// Handles peer registration. When a peer is created, it will register an applier.
func (a *applier) handleRegistration(reg *registration) {
	log.S().Infof("%s re-register to applier, term %d", a.tag, reg.term)
	y.Assert(a.id == reg.id)
	a.term = reg.term
	a.clearAllCommandsAsStale()
	*a = *newApplier(reg)
}

/// Handles apply tasks, and uses the applier to handle the committed entries.
func (a *applier) handleApply(aCtx *applyContext, apply *apply) {
	if aCtx.timer == nil {
		now := time.Now()
		aCtx.timer = &now
	}
	if len(apply.entries) == 0 || a.pendingRemove || a.stopped {
		return
	}
	a.metrics = applyMetrics{}
	a.term = apply.term
	a.handleRaftCommittedEntries(aCtx, apply.entries)
	for i := range apply.entries {
		apply.entries[i] = eraftpb.Entry{}
	}
	apply.entries = apply.entries[:0]
	if a.waitMergeState != nil {
		return
	}
	if a.pendingRemove {
		a.destroy(aCtx)
	}
}

/// Handles proposals, and appends the commands to the applier.
func (a *applier) handleProposal(regionProposal *regionProposal) {
	regionID, peerID := a.region.Id, a.id
	y.Assert(a.id == regionProposal.Id)
	if a.stopped {
		for _, p := range regionProposal.Props {
			cmd := pendingCmd{index: p.index, term: p.term, cb: p.cb}
			notifyStaleCommand(regionID, peerID, a.term, cmd)
		}
		return
	}
	for _, p := range regionProposal.Props {
		cmd := pendingCmd{index: p.index, term: p.term, cb: p.cb}
		if p.isConfChange {
			if confCmd := a.pendingCmds.takeConfChange(); confCmd != nil {
				// if it loses leadership before conf change is replicated, there may be
				// a stale pending conf change before next conf change is applied. If it
				// becomes leader again with the stale pending conf change, will enter
				// this block, so we notify leadership may have been changed.
				notifyStaleCommand(regionID, peerID, a.term, *confCmd)
			}
			a.pendingCmds.setConfChange(&cmd)
		} else {
			a.pendingCmds.appendNormal(cmd)
		}
	}
}

func (a *applier) destroy(aCtx *applyContext) {
	regionID := a.region.Id
	for _, res := range aCtx.applyTaskResList {
		if res.regionID == regionID {
			// Flush before destroying to avoid reordering messages.
			aCtx.flush()
		}
	}
	log.S().Infof("%s remove applier", a.tag)
	a.stopped = true
	for _, cmd := range a.pendingCmds.normals {
		notifyRegionRemoved(a.region.Id, a.id, cmd)
	}
	a.pendingCmds.normals = nil
	if cmd := a.pendingCmds.takeConfChange(); cmd != nil {
		notifyRegionRemoved(a.region.Id, a.id, *cmd)
	}
}

/// Handles peer destroy. When a peer is destroyed, the corresponding applier should be removed too.
func (a *applier) handleDestroy(aCtx *applyContext, regionID uint64) {
	if !a.stopped {
		a.destroy(aCtx)
		aCtx.applyResCh <- NewPeerMsg(MsgTypeApplyRes, a.region.Id, &applyTaskRes{
			regionID:      a.region.Id,
			destroyPeerID: a.id,
		})
	}
}

func (a *applier) resumePendingMerge(aCtx *applyContext) bool {
	return false // TODO: merge
}

func (a *applier) catchUpLogsForMerge(aCtx *applyContext, logs *catchUpLogs) {
	// TODO: merge
}

func (a *applier) handleTask(aCtx *applyContext, msg Msg) {
	switch msg.Type {
	case MsgTypeApply:
		a.handleApply(aCtx, msg.Data.(*apply))
	case MsgTypeApplyProposal:
		a.handleProposal(msg.Data.(*regionProposal))
	case MsgTypeApplyRegistration:
		a.handleRegistration(msg.Data.(*registration))
	case MsgTypeApplyDestroy:
		a.handleDestroy(aCtx, msg.RegionID)
	case MsgTypeApplyCatchUpLogs:
		a.catchUpLogsForMerge(aCtx, msg.Data.(*catchUpLogs))
	case MsgTypeApplyLogsUpToDate:
	}
}
