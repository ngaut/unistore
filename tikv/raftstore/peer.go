package raftstore

import (
	"time"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"fmt"
	"sync/atomic"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/coreos/etcd/raft"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/errors"
	"math"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb-test/_vendor/src/github.com/coreos/etcd/pkg/testutil"
	"encoding/binary"
	"github.com/ngaut/log"
	"./worker"
)

const (
	InvalidID = 0
)

type CoprocessorHost struct {
}

func (c *CoprocessorHost) PrePropose() error {
	// Todo
	return nil
}

type PollContext struct {
	Cfg *Config
	CoprocessorHost *CoprocessorHost
}

type StaleState int

const (
	StaleStateValid StaleState = 0 + iota
	StaleStateToValidate
	StaleStateLeaderMissing
)

type ReadResponse struct {
	Response raft_cmdpb.RaftCmdResponse
	Snapshot RegionSnapshot
}

type WriteResponse struct {
	Response raft_cmdpb.RaftCmdResponse
}

type Callback struct {
	ReadCb func(resp *ReadResponse)
	WriteCb func(resp *WriteResponse)
}

func (c *Callback)invokeWithResponse(resp *raft_cmdpb.RaftCmdResponse) {
	if c.ReadCb != nil {
		readResp := &ReadResponse{
			Response: resp,
			Snapshot: nil,
		}
		c.ReadCb(readResp)
	} else if c.WriteCb != nil {
		writeResp := &WriteResponse{ Response: resp }
		c.WriteCb(writeResp)
	}
}

type ReqCbPair struct {
	Req raft_cmdpb.RaftCmdRequest
	Cb Callback
}

type ReadIndexRequest struct {
	id uint64
	cmds []*ReqCbPair
	renewLeaseTime *time.Time
}

func NewReadIndexRequest(id uint64, cmds []*ReqCbPair, renewLeaseTime *time.Time) *ReadIndexRequest {
	return &ReadIndexRequest{
		id: id,
		cmds: cmds,
		renewLeaseTime: renewLeaseTime,
	}
}

func (r *ReadIndexRequest) bianryId() []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, r.id)
	return buf
}

type ReadIndexQueue struct {
	idAllocator uint64
	reads []*ReadIndexRequest
	readyCnt uint
}

func NewReadIndexQueue() *ReadIndexQueue {
	return &ReadIndexQueue{
		idAllocator: 0,
		reads: make([]*ReadIndexRequest, 0),
		readyCnt: 0,
	}
}

func (q *ReadIndexQueue) PopRead() *ReadIndexRequest {
	if len(q.reads) > 0 {
		req := q.reads[0]
		q.reads = q.reads[1:]
		return req
	}
	return nil
}


func NotifyStaleReq(term uint64, cb Callback) {
	resp := ErrResp(NewStaleCommandErr(), term)
	cb.invokeWithResponse(resp)
}

func (r *ReadIndexQueue) NextId() uint64 {
	r.idAllocator += 1
	return r.idAllocator
}

func (r *ReadIndexQueue) ClearUncommitted(term uint64) {
	uncommitted := r.reads[r.readyCnt:]
	r.reads = r.reads[:r.readyCnt]
	for _, read := range uncommitted {
		for _, reqCbPair := range read.cmds {
			NotifyStaleReq(term, reqCbPair.Cb)
		}
	}
}

type ProposalMeta struct {
	Index uint64
	Term uint64
	RenewLeaseTime *time.Time
}

type ProposalQueue struct {
	queue []*ProposalMeta
}

func newProposalQueue() *ProposalQueue {
	return &ProposalQueue {
		queue: make([]*ProposalMeta, 0),
	}
}


func (q *ProposalQueue) Pop(term uint64) *ProposalMeta {
	if len(q) == 0 || q.queue[0].Term > term{
		return nil
	}
	meta := q.queue[0]
	q.queue = q.queue[1:]
	return meta
}

func (q *ProposalQueue) Push(meta *ProposalMeta) {
	q.queue = append(q.queue, meta)
}

func (q *ProposalQueue) Clear() {
	q.queue = q.queue[:0]
}

const (
	ProposalContext_SyncLog ProposalContext = 1
	ProposalContext_Split ProposalContext = 1 << 1
	ProposalContext_PrepareMerge ProposalContext = 1 << 2
)

type ProposalContext byte

func newEmptyProposalContext() *ProposalContext {
	return &ProposalContext{}
}

func (c *ProposalContext) ToVec() []byte {
	var res []byte
	res = append(res, *c)
	return res
}

func NewProposalContextFromBytes(ctx []byte) ProposalContext {
	l := len(ctx)
	if l == 0 {
		return nil
	} else if l == 1 {
		return ctx[0]
	} else {
		panic(fmt.Sprintf("Invalid ProposalContext %v", ctx))
	}
}

func (c *ProposalContext) contains(flag ProposalContext) bool {
	return *c & flag
}

func (c *ProposalContext) insert(flag ProposalContext) {
	*c |= flag
}

type PeerStat struct {
	WrittenBytes uint64
	WrittenKeys uint64
}

func NewPeerStat() *PeerStat {
	return &PeerStat {
		WrittenBytes: 0,
		WrittenKeys: 0,
	}
}

type WaitApplyResultStat struct {
	Results []*ApplyTaskRes
	ReadyToMerge atomic.Value
}

type Lease struct {

}

func NewLease(duration time.Duration) *Lease {
	return &Lease {

	}
}

type Proposal struct {
	isConfChange bool
	index uint64
	term uint64
	Cb Callback
}

type RegionProposal struct {
	Id uint64
	RegionId uint64
	Props []*Proposal
}

func NewRegionProposal(id uint64, regionId uint64, props []*Proposal) *RegionProposal {
	return &RegionProposal {
		Id: id,
		RegionId: regionId,
		Props: props,
	}
}

type RecentAddedPeer struct {
	RejectDurationAsSecs uint64
	Id uint64
	AddedTime time.Time
}

func NewRecentAddedPeer(rejectDurationAsSecs uint64) *RecentAddedPeer {
	return &RecentAddedPeer{
		RejectDurationAsSecs: rejectDurationAsSecs,
		Id: 0,
		AddedTime: time.Now(),
	}
}

func (r *RecentAddedPeer) Update(id uint64, now time.Time) {
	r.Id = id
	r.AddedTime = now
}

func (r *RecentAddedPeer) Contains(id uint64) bool {
	if r.Id == id {
		now := time.Now()
		elapsedSecs := now.Sub(r.AddedTime).Seconds()
		return elapsedSecs < r.RejectDurationAsSecs
	}
	return false
}

type Peer struct {
	Cfg *Config
	peerCache map[uint64]*metapb.Peer
	Peer *metapb.Peer
	regionId uint64
	RaftGroup *raft.RawNode
	peerStorage *PeerStorage
	proposals *ProposalQueue
	applyProposals []*Proposal
	pendingReads *ReadIndexQueue

	// Record the last instant of each peer's heartbeat response.
	PeerHeartbeats map[uint64]time.Time

	/// Record the instants of peers being added into the configuration.
	/// Remove them after they are not pending any more.
	PeersStartPendingTime map[uint64]time.Time
	RecentAddedPeer RecentAddedPeer

	SizeDiffHint uint64
	deleteKeysHint uint64
	ApproximateSize *uint64
	ApproximateKeys *uint64
	CompactionDeclinedBytes uint64
	Tag string

	LastApplyingIdx uint64
	LastCompactedIdx uint64
	lastUrgentProposalIdx uint64
	lastCommittedSplitIdx uint64
	RaftLogSizeHint uint64
	PendingRemove bool

	// The index of the latest committed prepare merge command.
	lastCommittedPrepareMergeIdx uint64
	PendingMergeState *raft_serverpb.MergeState
	leaderMissingTime *time.Time
	leaderLease Lease

	pendingMessages []eraftpb.Message
	PendingMergeApplyResult *WaitApplyResultStat
	PeerStat *PeerStat
}

func NewPeer(storeId uint64, cfg *Config, engines *Engines, region *metapb.Region, peer metapb.Peer) (*Peer, error) {
	if peer.GetId() == InvalidID {
		return nil, errors.New("Invalid peer id")
	}
	tag := fmt.Sprintf("[region %v] %v", region.GetId(), peer.GetId())
	ps := NewPeerStorage(engines, region, tag)
	appliedIndex := ps.AppliedIndex()

	raftCfg := &raft.Config {
		ID: peer.GetId(),
		ElectionTick: cfg.RaftElectionTimeoutTicks,
		HeartbeatTick: cfg.RaftHeartbeatTicks,
		MaxSizePerMsg: cfg.RaftMaxSizePerMsg,
		MaxInflightMsgs: cfg.RaftMaxInflightMsgs,
		Applied: appliedIndex,
		CheckQuorum: true,
		PreVote: cfg.Prevote,
		Storage: ps,
	}

	raftGroup, err := raft.NewRawNode(raftCfg, nil)
	if err != nil {
		return nil, err
	}
	peer := &Peer {
		Cfg: cfg,
		Peer: peer,
		regionId: region.GetId(),
		RaftGroup: raftGroup,
		peerStorage: ps,
		proposals: newProposalQueue(),
		applyProposals: make([]*Proposal, 0),
		pendingReads: NewReadIndexQueue(),
		peerCache: make(map[uint64]metapb.Peer),
		PeerHeartbeats: make(map[uint64]time.Time),
		PeersStartPendingTime: make(map[uint64]time.Time),
		SizeDiffHint: 0,
		deleteKeysHint: 0,
		ApproximateSize: nil,
		ApproximateKeys: nil,
		CompactionDeclinedBytes: 0,
		PendingRemove: false,
		PendingMergeState: nil,
		lastCommittedPrepareMergeIdx: 0,
		leaderMissingTime: &time.Now(),
		Tag: tag,
		LastApplyingIdx: appliedIndex,
		LastCompactedIdx: 0,
		lastUrgentProposalIdx: math.MaxInt64,
		lastCommittedSplitIdx: 0,
		RaftLogSizeHint: 0,
		leaderLease: NewLease(cfg.RaftStoreMaxLeaderLease),
		pendingMessages: make([]*eraftpb.Message, 0),
		PendingMergeApplyResult: nil,
		PeerStat: NewPeerStat(),
	}

	if len(region.GetPeers()) == 1 && region.GetPeers()[0].GetStoreId() == storeId {
		err := peer.RaftGroup.Campaign()
		if err != nil {
			return nil, err
		}
	}

	return peer, nil
}

func (p *Peer) Activate() {
	panic("unimplemented")
}

func (p *Peer) nextProposalIndex() uint64 {
	p.peerStorage.LastIndex() + 1
}

func (p *Peer) MaybeDestroy() *DestroyPeerJob {
	if p.PendingRemove {
		return nil
	}
	initialized := p.peerStorage.isInitialized()

}

func (p *Peer) Destroy(keepData bool) error {
	t = time.Now()

}

func (p *Peer) isInitialized() bool {
	return p.peerStorage.isInitialized()
}

func (p *Peer) Region() *metapb.Region {
	return p.peerStorage.Region()
}

func (p *Peer) PeerId() uint64 {
	return p.Peer.GetId()
}

func (p *Peer) GetRaftStatus() {
	p.RaftGroup.Status()
}

func (p *Peer) LeaderId() uint64 {
	// TODO etcd raft doesn't export member raft
	return 0
}

func (p *Peer) IsLeader() bool {
	// TODO etcd raft doesn't export member raft
	return false
}

func (p *Peer) GetRole()  raft.StateType {
	// TODO etcd raft doesn't export member raft
	return 0
}

func (p *Peer) Store() *PeerStorage {
	p.peerStorage
}

func (p *Peer) IsApplyingSnapshot() bool {
	p.peerStorage.IsApplyingSnapshot()
}

func (p *Peer) HasPendingSnapshot() bool {
	// TODO etcd raft doesn't export member raft
	return false
}

func (p *Peer) Send(msgs []*eraftpb.Message) error {
	for _, msg := range msgs {
		err := p.snedRaftMessage(msg)
		if err != nil {
			return err
		}
	}
}

func (p *Peer) Step(m *eraftpb.Message) error {
	if p.IsLeader() && m.GetFrom() != InvalidID {
		p.PeerHeartbeats[m.GetFrom()] = time.Now()
		p.leaderMissingTime = nil
	} else if m.GetFrom() == p.LeaderId() {
		p.leaderMissingTime = nil
	}
	return p.RaftGroup.Step(m)
}

func (p *Peer) CheckPeers() {
	if !p.IsLeader() {
		p.PeerHeartbeats = make(map[uint64]time.Time)
		return
	}
	if len(p.PeerHeartbeats) == len(p.Region().GetPeers()) {
		return
	}
	region := p.Region()
	for _, peer := range region.GetPeers() {
		p.PeerHeartbeats[peer.GetId()] = time.Now()
		time.Time
	}
}

func (p *Peer) CollectDownPeers(maxDuration time.Duration) []*pdpb.PeerStats {
	downPeers := make([]*pdpb.PeerStats, 0)
	for _, peer := p.Region().GetPeers() {
		if peer.GetId() == p.Peer.GetId() {
			continue
		}
		if hb, ok := p.PeerHeartbeats[peer.GetId()]; ok {
			if time.Since(hb) > maxDuration {
				stats := &pdpb.PeerStats{
					Peer: peer,
					DownSeconds: uint64(time.Since(hb).Seconds()),
				}
				downPeers = append(downPeers, stats)
			}
		}
	}
	return downPeers
}

func (p *Peer) CollectPendingPeers() []*metapb.Peer {
	pendingPeers := make([]*metapb.Peer, 0, len(p.Region().GetPeers()))
	status := p.RaftGroup.Status()
	truncatedIdx := p.Store().truncatedIndex()

	for id, progress := range status.Progress {
		if id == p.Peer.GetId() {
			continue
		}
		if progress.Match < truncatedIdx {
			if peer := p.GetPeerFromCache(id); peer != nil {
				pendingPeers = append(pendingPeers, peer)
				for peerId, startPendingTime := range p.PeersStartPendingTime {
					if peerId == id {
						p.PeersStartPendingTime[id] = time.Now()
						break
					}
				}
			}
		}
	}
	return pendingPeers
}


func (p *Peer) clearPeersStartPendingTime() {
	for k := range p.PeersStartPendingTime {
		delete (p.PeersStartPendingTime, k)
	}
}

/// Returns `true` if any new peer catches up with the leader in replicating logs.
/// And updates `PeersStartPendingTime` if needed.
func (p *Peer) AnyNewPeerCatchUp(peerId uint64) bool {
	if len(p.PeersStartPendingTime) == 0 {
		return false
	}
	if !p.IsLeader() {
		p.clearPeersStartPendingTime()
		return false
	}
	for id, _ := range p.PeersStartPendingTime {
		if id == peerId {
			continue
		}
		truncatedIdx := p.Store().truncatedIndex()
		if progress, ok := p.RaftGroup.raft.prs[peerId]; ok {
			if progress.Matched >= truncatedIdx {
				delete(p.PeersStartPendingTime, id)
			}
			return true
		}
	}
	return false
}

func (p *Peer) CheckStaleState() StaleState {
	if p.IsLeader() {
		p.leaderMissingTime = nil
		return StaleStateValid
	}
	naivePeer := !p.isInitialized() || p.RaftGroup.raft.isLeaner()
	if p.leaderMissingTime == nil {
		p.leaderMissingTime = &time.Now()
		return StaleStateValid
	} else {
		if time.Since(p.leaderMissingTime) >= p.Cfg.MaxLeaderMissingDuration {
			p.leaderMissingTime = &time.Now()
			return StaleStateToValidate
		} else if time.Since(p.leaderMissingTime) >= p.Cfg.AbnormalLeaderMissingDuration && !naivePeer {
			return StaleStateLeaderMissing
		}
		return StaleStateValid
	}
}

// TODO: finish the function after local reader implemented
func (p *Peer) OnRoleChanged(ready *raft.Ready) {
	ss := ready.SoftState
	if ss != nil {
		if ss.RaftState == raft.StateFollower {

		} else if ss.RaftState == raft.StateFollower {

		}
	}
}

func (p *Peer) ReadyToHandlePendingSnap() bool {
	return p.LastApplyingIdx == p.Store().AppliedIndex()
}

func (p *Peer) readyToHandleRead() bool {
	return p.Store().appliedIndexTerm == p.Term() && !p.isSplitting() && !p.isMerging()
}

func (p *Peer) isSplitting() bool {
	return p.lastCommittedSplitIdx > p.Store().AppliedIndex()
}

func (p *Peer) isMerging() bool {
	return p.lastCommittedPrepareMergeIdx > p.Store().AppliedIndex() || p.PendingMergeState != nil
}

func (p *Peer) TakeApplyProposals() *RegionProposal {
	if len(p.applyProposals) == 0 {
		return nil
	}
	props := p.applyProposals
	p.applyProposals = make([]*Proposal, 0)
	return NewRegionProposal(p.PeerId(), p.regionId, props)
}

func (p *Peer) HandleRaftReadyAppend() {
	if p.PendingRemove {
		return
	}
	if p.Store().CheckApplySnap() {
		return
	}
	if len(p.pendingMessages) > 0 {
		messages := p.pendingMessages
		p.pendingMessages = make([]*eraftpb.Message, 0)
		p.Send(messages)
	}
	if p.HasPendingSnapshot() && !p.ReadyToHandlePendingSnap() {
		return
	}
	if !p.RaftGroup.HasReadySince(p.LastApplyingIdx) {
		return
	}
	ready := p.RaftGroup.ReadySince(p.LastApplyingIdx)
	p.OnRoleChanged(&ready)

	if p.IsLeader() {
		p.Send(ready.Messages)
		ready.Messages = ready.Messages[:0]
	}
	invokeCtx, err := p.Store().HandleRaftReady(&ready)
	if err != nil {
		panic(fmt.Sprintf("failed to handle raft ready, error: %v", err))
	}
	// ctx.ready_res.push((ready, invokeCtx))
}

func (p *Peer) PostRaftReadyAppend(ctx PollContext, ready *raft.Ready, invokeCtx InvokeContext) *ApplySnapResult {
	if invokeCtx.hasSnapshot() {
		p.RaftLogSizeHint = 0
	}
	applySnapResult := p.Store().PostReady(invokeCtx)
	if applySnapResult != nil && p.Peer.GetIsLearner() {
		var pr metapb.Peer
		for _, peer := range p.Region().GetPeers() {
			if peer.GetId() == p.Peer.GetId() {
				pr = peer
			}
		}
		if pr != p.Peer {
			p.Peer = pr
		}
	}
	if !p.IsLeader() {
		if p.IsApplyingSnapshot() {
			p.pendingMessages = ready.Messages
			ready.Messages = nil
		} else {
			p.Send(ready.Messages)
			ctx.NeedFlushTrans = true
		}
	}
	if applySnapResult != nil {
		p.Activate(ctx)
	}
	return applySnapResult
}

func (p *Peer) HandleRaftReadyApply(ctx PollContext, ready *raft.Ready) {
	if p.IsApplyingSnapshot() {
		p.LastApplyingIdx = p.Store().truncatedIndex()
	} else {
		committedEntries := ready.CommittedEntries
		ready.CommittedEntries = nil
		leaseToBeUpdated, splitToBeUpdated, mergeToBeUpdated := p.IsLeader(), p.IsLeader(), p.IsLeader()
		if !leaseToBeUpdated {
			p.proposals := p.proposals.Clear()
		}
		for _, entry := range committedEntries {
			p.RaftLogSizeHint += len(entry.Data)
			if leaseToBeUpdated {
				proposeTime := p.findProposeTime(entry.Index, entry.Term)
				if proposeTime != nil {
					p.MaybeRenewLeaderLease(ctx.localReader, proposeTime)
					leaseToBeUpdated = false
				}
			}

			if entry.Term == p.Term() && (splitToBeUpdated || mergeToBeUpdated) {
				ctx := NewProposalContextFromBytes(entry.Context)
				if splitToBeUpdated && ctx.contains(ProposalContext_Split) {
					p.lastCommittedSplitIdx = entry.Index
					splitToBeUpdated = false
				}
				if mergeToBeUpdated && ctx.contains(ProposalContext_PrepareMerge) {
					p.lastCommittedPrepareMergeIdx = entry.Index
					p.leaderLease.suspect(MonotonicRawNow())
					mergeToBeUpdated = false
				}
			}
		}

		l := len(committedEntries)
		if l > 0 {
			p.LastApplyingIdx = committedEntries[l-1].Index
			if p.LastApplyingIdx >= p.lastUrgentProposalIdx {
				p.RaftGroup.SkipBcastCommit(true)
				p.lastUrgentProposalIdx = math.MaxUint64
			}
			apply := &Apply{
				RegionId: p.regionId,
				Term: p.Term(),
				Entries: committedEntries,
			}
			p.ApplyRouter.ScheduleTask(p.regionId, apply)
		}

		p.ApplyReads(ctx, ready)

		p.RaftGroup.AdvanceAppend(ready)
		if p.IsApplyingSnapshot() {
			p.RaftGroup.AdvanceApply(p.LastApplyingIdx)
		}
	}
}

func (p *Peer) ApplyReads(ctx *PollContext, ready *raft.Ready) {
	var proposeTime *time.Time
	if p.readyToHandleRead() {
		for _, state := range ready.ReadStates {
			read := p.pendingReads.PopRead()
			if read == nil {
				panic("read should exist")
			}
			if state.RequestCtx != read.bianryId() {
				panic(fmt.Sprintf("request ctx: %v not equal to read id: %v", state.RequestCtx, read.bianryId()))
			}
			for _, reqCb := range read.cmds {
				reqCb.Cb.invokeWithResponse(p.HandleRead(ctx, reqCb.Req, true));
			}
			read.cmds = nil
			proposeTime = read.renewLeaseTime
		}
	} else {
		for _, state := range ready.ReadStates {
			read := p.pendingReads.reads[p.pendingReads.readyCnt]
			if state.RequestCtx != read.bianryId() {
				panic(fmt.Sprintf("request ctx: %v not equal to read id: %v", state.RequestCtx, read.bianryId()))
			}
			p.pendingReads.readyCnt += 1
			proposeTime = read.renewLeaseTime
		}
	}

	if ready.SoftState != nil {
		p.pendingReads.ClearUncommitted(p.Term())
	}

	if proposeTime != nil {
		if p.leaderLease.Inspect(proposeTime) == LeaseState_Suspect {
			return
		}
		p.MaybeRenewLeaderLease(ctx.LocalReader, proposeTime)
	}
}

func (p *Peer) PostApply(ctx *PollContext, applyState raft_serverpb.RaftApplyState, appliedIndexTerm uint64, merged bool, applyMetrics *ApplyMetrics) bool {
	hasReady := false
	if p.IsApplyingSnapshot() {
		panic("should not applying snapshot")
	}

	if !merged {
		p.RaftGroup.AdvanceApply(applyState.AppliedIndex)
	}

	progressToBeUpdated := p.Store().appliedIndexTerm != appliedIndexTerm
	p.Store().applyState = applyState
	p.Store().appliedIndexTerm = appliedIndexTerm

	p.PeerStat.WrittenBytes += applyMetrics.WrittenBytes
	p.PeerStat.WrittenKeys += applyMetrics.WrittenKeys
	p.deleteKeysHint += applyMetrics.DeleteKeysHint
	diff := p.SizeDiffHint + applyMetrics.SizeDiffHint
	p.SizeDiffHint = math.MaxUint64(diff, 0)

	if p.HasPendingSnapshot() && p.ReadyToHandlePendingSnap() {
		hasReady = true
	}

	if p.pendingReads.readyCnt > 0 && p.readyToHandleRead() {
		for i := 0; i < p.pendingReads.readyCnt; i++ {
			read := p.pendingReads.PopRead()
			if read == nil {
				panic("read is nil, this should not happen")
			}
			for _, reqCb := range read.cmds {
				reqCb.Cb(p.HandleRead(ctx, reqCb.Req, true))
			}
		}
		p.pendingReads.readyCnt = 0
	}

	if progressToBeUpdated && p.IsLeader() {
		progress = ReadProgress::appliedIndexTerm(appliedIndexTerm)
		p.MaybeUpdateReadProgress(ctx.LocalReader, progress)
	}
	return hasReady
}

func (p *Peer) PostSplit() {
	p.deleteKeysHint = 0
	p.SizeDiffHint = 0
}

func (p *Peer) MaybeUpdateReadProgress(reader chan ReadTask, ts time.Time) {
	if !p.IsLeader() || p.isSplitting() || p.isMerging() {
		return
	}
	p.leaderLease.Renew(ts)
	remoteLease := p.leaderLease.MaybeNewRemoteLease(p.Term())
	if remoteLease != nil {
		progress := ReadProgress::leader_lease(remoteLease);
		p.MaybeUpdateReadProgress(reader, progress)
	}
}

func (p *Peer) MaybeUpdateReadProgress(localReader chan ReadTask, progress ReadProgress) {
	if p.PendingRemove {
		return
	}
	update := ReadTask::update(p.regionId, progress)
	localReader <- update
}

func (p *Peer) MaybeCampaign(parentIsLeader bool) bool {
	if len(p.Region().GetPeers()) <= 1 || !parentIsLeader{
		return false
	}

	p.RaftGroup.Campaign()
	return true
}

func IsUrgentRequest(req *raft_cmdpb.RaftCmdRequest) bool {
	if req.AdminRequest == nil {
		return false
	}
	switch req.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_Split,
			raft_cmdpb.AdminCmdType_BatchSplit,
			raft_cmdpb.AdminCmdType_ChangePeer,
			raft_cmdpb.AdminCmdType_ComputeHash,
			raft_cmdpb.AdminCmdType_VerifyHash,
			raft_cmdpb.AdminCmdType_PrepareMerge,
			raft_cmdpb.AdminCmdType_CommitMerge,
			raft_cmdpb.AdminCmdType_RollbackMerge:
		    return true
		default:
		    return false
	}
}

const (
	RequestPolicy_ReadLocal int = 0 + iota
	RequestPolicy_ReadIndex
	RequestPolicy_ProposeNormal
	RequestPolicy_ProposeTransferLeader
	RequestPolicy_ProposeConfChange
)

func (p *Peer) Propose(ctx *PollContext, cb Callback, req raft_cmdpb.RaftCmdRequest, errResp *raft_cmdpb.RaftCmdResponse) bool {
	if p.PendingRemove {
		return false
	}
	isConfChange := false
	isUrgent := IsUrgentRequest(&req)

	policy, err := p.Inspect(&req)
	if err != nil {
		BindError(errResp, err)
		cb.invokeWithResponse(errResp)
		return false
	}
	var idx uint64
	var err error
	switch policy {
	case RequestPolicy_ReadLocal:
		p.ReadLocal(ctx, req, cb)
		return false
	case RequestPolicy_ReadIndex:
		return p.ReadIndex(ctx, req, errResp, cb)
	case RequestPolicy_ProposeNormal:
		idx, err = p.ProposeNormal(ctx, req)
	case RequestPolicy_ProposeTransferLeader:
		return p.ProposeTransferLeader(ctx, req, cb)
	case RequestPolicy_ProposeConfChange:
		isConfChange = true
		idx, err = p.ProposeConfChange(ctx, req)
	}
	if err != nil {
		BindError(errResp, err)
		cb.invokeWithResponse(errResp)
		return false
	}
	if isUrgent {
		p.lastUrgentProposalIdx = idx
		p.RaftGroup.SkipBcastCommit(false)
	}
	meta := &ProposalMeta{
		Index: idx,
		Term: p.Term(),
		RenewLeaseTime: nil,
	}
	p.PostPropose(meta, isConfChange, cb)
	return true
}

func (p *Peer) PostPropose(meta *ProposalMeta, isConfChange bool, cb Callback) {
	meta.RenewLeaseTime = MonotonicRawNow()
	proposal := &Proposal{
		IsConfChange: isConfChange,
		Index: meta.Index,
		Term: meta.Term,
		Cb: cb
	}

}

func (p *Peer) countHealthyNode(progress map[uint64]raft.Progress) int {
	healthy := 0
	for _, pr := range progress {
		if pr.Match >= p.Store().truncatedIndex() {
			healthy += 1
		}
	}
	return healthy
}

func (p *Peer) checkConfChange(ctx *PollContext, cmd *raft_cmdpb.RaftCmdRequest) error {
	changePeer := worker.GetChangePeerCmd(cmd)
	if changePeer == nil {
		panic("Change Peer is nil")
	}
	changeType := changePeer.GetChangeType()
	peer := changePeer.GetPeer()

	// Check the request itself is valid or not.
	if (changeType == eraftpb.ConfChangeType_AddNode && peer.IsLearner) ||
		(changeType == eraftpb.ConfChangeType_AddLearnerNode && !peer.IsLearner) {
		log.Warnf("%s conf change type: %v, but got peer %v", p.Tag, changeType, peer)
		return fmt.Errorf("invalid conf change request")
	}

	if changeType == eraftpb.ConfChangeType_RemoveNode && !ctx.Cfg.AllowRemoveLeader && peer.Id == p.PeerId() {
		log.Warnf("%s rejects remove leader request %v", p.Tag, changePeer)
		return fmt.Errorf("ignore remove leader")
	}

	status := p.RaftGroup.Status()
	total := len(status.Progress)
	if total == 1 {
		// It's always safe if there is only one node in the cluster.
		return nil
	}

	switch changeType {
	case eraftpb.ConfChangeType_AddNode:
		if pr, ok := status.LearnerProgress[peer.Id]; ok {
			pr.IsLearner = false
			status.Progress[peer.Id] = pr
			delete(status.LearnerProgress, peer.Id)
		} else {
			status.Progress[peer.Id] = raft.Progress{}
		}
	case eraftpb.ConfChangeType_RemoveNode:
		if peer.GetIsLearner() {
			return nil
		}
		if _, ok := status.Progress[peer.Id]; ok {
			return nil
		}
	case eraftpb.ConfChangeType_AddLearnerNode:
		return nil
	}
	healthy := p.countHealthyNode(status.Progress)
	quorumAfterChange := Quorum(len(status.Progress))
	if healthy >= quorumAfterChange {
		return nil
	}

	return fmt.Errorf("unsafe to perform conf change %v, total %v, healthy %v, quorum after chagne %v",
	changePeer, total, healthy, quorumAfterChange)
}

func Quorum(total int) int {
	return total/2 + 1
}

func (p *Peer) transferLeader(peer *metapb.Peer) {
	log.Infof("%v transfer leader to %v", p.Tag, peer)

	p.RaftGroup.TransferLeader(peer.GetId())
}

func (p *Peer) readyToTransferLeader(ctx *PollContext, peer *metapb.Peer) bool {
	peerId := peer.GetId()
	status := p.RaftGroup.Status()

	if _, ok := status.Progress[peerId]; !ok {
		return false
	}

	for _, pr := range status.Progress {
		if pr.State == raft.ProgressStateSnapshot {
			return false
		}
	}
	if p.RecentAddedPeer.Contains(peerId) {
		log.Debugf("%v reject tranfer leader to %v due to the peer was added recently", p.Tag, peer)
		return false
	}

	lastIndex, _ := p.Store().LastIndex()

	return lastIndex <= status.Progress[peerId].Match + ctx.Cfg.LeaderTransferMaxLogLag
}

func (p *Peer) readLocal(ctx *PollContext, req *raft_cmdpb.RaftCmdRequest, cb Callback) {
	cb.invokeWithResponse(p.HandleRead(ctx, req, false))
}

func (p *Peer) preReadIndex() error {
	if p.isSplitting() {
		return fmt.Errorf("can not read index due to split")
	}
	if p.isMerging() {
		return fmt.Errorf("can not read index due to merge")
	}
	return nil
}

// Returns a boolean to indicate whether the `read` is proposed or not.
// For these cases it won't be proposed:
// 1. The region is in merging or splitting;
// 2. The message is stale and dropped by the Raft group internally;
// 3. There is already a read request proposed in the current lease;
func (p *Peer) readIndex(pollCtx *PollContext, req *raft_cmdpb.RaftCmdRequest, errResp *raft_cmdpb.RaftCmdResponse, cb Callback) bool {
	err := p.preReadIndex()
	if err != nil {
		log.Debugf("%v prevents unsafe read index, err: %v", p.Tag, err)
		BindError(errResp, e)
		cb.invokeWithResponse(errResp)
		return false
	}

	renewLeaseTime := &time.Now()
	readsLen := len(p.pendingReads.reads)
	if readsLen > 0 {
		read := p.pendingReads.reads[readsLen-1]
		if read.renewLeaseTime + pollCtx.Cfg.RaftStoreMaxLeaderLease > renewLeaseTime {
			read.cmds.push(&ReqCbPair{Req:req, Cb:cb})
			return false
		}
	}

	lastPendingReadCount := p.RaftGroup.Raft.PendingReadCount
	lastReadyReadCount := p.RaftGroup.Raft.ReadyReadCount

	id := p.pendingReads.NextId()
	b := make([]byte, 8)
	ctx := binary.BigEndian.PutUint64(b, id)
	p.RaftGroup.ReadIndex(b)

	pendingReadCount := p.RaftGroup.Raft.PendingReadCount
	readyReadCount := p.RaftGroup.Raft.ReadyReadCount

	if pendingReadCount == lastPendingReadCount && readyReadCount == lastReadyReadCount {
		NotifyStaleReq(p.Term(), cb)
		return false
	}

	cmds := make([]ReqCbPair, 0, 1)
	cmds = append(cmds, &ReqCbPair{req, cb})
	p.pendingReads.reads = append(p.pendingReads.reads, NewReadIndexRequest(id, cmds, renewLeaseTime))

	// TimeoutNow has been sent out, so we need to propose explicitly to
	// update leader lease.
	if p.leaderLease.Inspect(renewLeaseTime) == LeaseState_Suspect {
		req := raft_cmdpb.RaftCmdRequest{}
		if index, err := p.ProposeNormal(pollCtx, req); err == nil {
			meta := &ProposalMeta {
				Index: index,
				Term: p.Term(),
				RenewLeaseTime: renewLeaseTime,
			}
			p.PostPropose(meta, false, Callback{})
		}
	}

	return true
}

func (p *Peer) GetMinProgress() uint64 {
	minMatch := math.MaxUint64
	hasProgress := false
	for _, pr := range p.RaftGroup.Status().Progress {
		hasProgress = true
		if pr.Match < minMatch {
			minMatch = pr.Match
		}
	}
	if !hasProgress {
		return 0
	}
	return minMatch
}

func (p *Peer) preProposePrepareMerge(ctx *PollContext, req *raft_cmdpb.RaftCmdRequest) error {
	lastIndex := p.RaftGroup.Raft.RaftLog.LastIndex()
	minProgress := p.GetMinProgress()
	minIndex := minProgress + 1
	if minProgress == 0 || lastIndex - minProgress > ctx.Cfg.MergeMaxLogGap {
		return fmt.Errorf("log gap (%v, %v] is too large, skip merge", minProgress, lastIndex)
	}

	entrySize := 0
	for _, entry := range p.RaftGroup.Raft.RaftLog.Entries(minIndex, math.MaxUint64) {
		entrySize += len(entry.GetData())
		if entry.GetEntryType() == eraftpb.EntryType_EntryConfChange {
			return fmt.Errorf("log gap contains conf change, skip merging.")
		}
		if len(entry.GetData()) == 0 {
			continue
		}
		cmd := &raft_cmdpb.RaftCmdRequest{}
		err := cmd.Unmarshal(entry.GetData())
		if err != nil {
			panic("%v data is corrupted at %v, error: %v", p.Tag, entry.GetIndex(), err)
		}
		if cmd.AdminRequest == nil {
			continue
		}
		cmdType := cmd.AdminRequest.GetCmdType()
		switch cmdType {
			case raft_cmdpb.AdminCmdType_TransferLeader, raft_cmdpb.AdminCmdType_ComputeHash,
			raft_cmdpb.AdminCmdType_VerifyHash, raft_cmdpb.AdminCmdType_InvalidAdmin:
			continue
		default:
		}

		return fmt.Errorf("log gap contains admin request %v, skip merging.", cmdType)
	}

	if entrySize > ctx.Cfg.RaftEntryMaxSize * 0.9 {
		return fmt.Errorf("log gap size exceed entry size limit, skip merging.")
	}

	req.AdminRequest.PrepareMerge.MinIndex = minIndex
	return nil
}

func (p *Peer) PrePropose(pollCtx *PollContext, req *raft_cmdpb.RaftCmdRequest) (*ProposalContext, error) {
	pollCtx.CoprocessorHost.PrePropose(p.Region(), req)
	ctx := newEmptyProposalContext()

	if getSyncLogFromRequest(req) {
		ctx.insert(ProposalContext_SyncLog)
	}

	if req.AdminRequest == nil {
		return ctx, nil
	}

	switch req.AdminRequest.GetCmdType() {
	case raft_cmdpb.AdminCmdType_Split, raft_cmdpb.AdminCmdType_BatchSplit:
		ctx.insert(ProposalContext_Split)
	default:
	}

	if req.AdminRequest.PrepareMerge != nil {
		err := p.preProposePrepareMerge(pollCtx, req)
		if err != nil {
			return nil, err
		}
		ctx.insert(ProposalContext_PrepareMerge)
	}

	return ctx, nil
}

func (p *Peer) ProposeNormal(pollCtx *PollContext, req *raft_cmdpb.RaftCmdRequest) (uint64, error) {
	if p.PendingMergeState != nil && req.AdminRequest.CmdType != raft_cmdpb.AdminCmdType_RollbackMerge {
		return fmt.Errorf("peer in merging mode, can't do proposal.")
	}

	ctx, err := p.PrePropose(pollCtx, req)
	if err != nil {
		log.Warnf("%v skip proposal: %v", p.Tag, err);
		return 0, err
	}
	data, err := req.Marshal()
	if err != nil {
		return 0, err
	}

	if len(data) > pollCtx.Cfg.RaftEntryMaxSize {
		log.Errorf("entry is too large, entry size %v", len(data));
		return 0, &ErrRaftEntryTooLarge{RegionId: p.regionId, DataLen: len(data)}
	}

	proposeIndex := p.nextProposalIndex()
	err = p.RaftGroup.Propose(ctx.ToVec(), data)
	if err != nil {
		return 0, err
	}
	if proposeIndex == p.nextProposalIndex() {
		return 0, &ErrNotLeader{RegionId: p.regionId}
	}

	return proposeIndex, nil
}

// Return true if the transfer leader request is accepted.
func (p *Peer) ProposeTransferLeaser(ctx *PollContext, req *raft_cmdpb.RaftCmdRequest, cb Callback) bool {
	transferLeader := getTransferLeaderCmd(req)
	if transferLeader == nil {
		panic!("transfer leader is nil")
	}
	peer := transferLeader.Peer

	transferred := false
	if p.readyToTransferLeader(ctx, peer) {
		p.transferLeader(peer)
		transferred = true
	} else {
		log.Infof("%v transfer leader message %v ignored directly", p.Tag, req)
		transferred = false
	}

	// transfer leader command doesn't need to replicate log and apply, so we
	// return immediately. Note that this command may fail, we can view it just as an advice
	cb.invokeWithResponse(makeTransferLeaderResponse())

	return transferred
}

// Fails in such cases:
// 1. A pending conf change has not been applied yet;
// 2. Removing the leader is not allowed in the configuration;
// 3. The conf change makes the raft group not healthy;
// 4. The conf change is dropped by raft group internally.
func (p *Peer) ProposeConfChange(ctx *PollContext, req *raft_cmdpb.RaftCmdRequest) (uint64, error) {
	if p.PendingMergeState != nil {
		return 0, fmt.Errorf("peer in merging mode, can't do proposal.")
	}

	if p.RaftGroup.Raft.PendingConfIndex > p.Store().AppliedIndex() {
		log.Infof("%v there is a pending conf change, try later", p.Tag)
		return 0, fmt.Errorf("%v there is a pending conf change, try later", p.Tag)
	}

	if err := p.checkConfChange(ctx, req); err != nil {
		return 0, err
	}

	data, err := req.Marshal()
	if err != nil {
		return 0, err
	}

	changePeer := worker.GetChangePeerCmd(req)
	if changePeer == nil {
		panic("Change Peer should not be nil")
	}
	cc := &eraftpb.ConfChange{}
	cc.ChangeType = changePeer.ChangeType
	cc.NodeId = changePeer.Peer.Id
	cc.Context = data

	log.Infof("%v propose conf change %v peer %v", p.Tag, cc.ChangeType, cc.NodeId)

	proposeIndex := p.nextProposalIndex()
	if err = p.RaftGroup.ProposeConfChange(ProposalContext_SyncLog.ToVec(), cc); err != nil {
		return 0, err
	}
	if p.nextProposalIndex() == proposeIndex {
		return 0, &ErrNotLeader{}
	}

	return proposeIndex, nil
}

func (p *Peer) handleRead(ctx *PollContext, req *raft_cmdpb.RaftCmdRequest, checkEpoch bool) *ReadResponse {

}

func getTransferLeaderCmd(req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.TransferLeaderRequest {
	if req.AdminRequest == nil {
		return nil
	}
	return req.AdminRequest.TransferLeader
}

func makeTransferLeaderResponse() *raft_cmdpb.RaftCmdResponse {
	adminResp := &raft_cmdpb.AdminResponse{}
	adminResp.CmdType = raft_cmdpb.AdminCmdType_TransferLeader
	adminResp.TransferLeader = &raft_cmdpb.TransferLeaderResponse{}
	resp := &raft_cmdpb.RaftCmdResponse{}
	resp.AdminResponse = adminResp
	return resp
}

func getSyncLogFromRequest(req *raft_cmdpb.RaftCmdRequest) bool {
	if req.AdminRequest != nil {
		switch req.AdminRequest.GetCmdType() {
		case raft_cmdpb.AdminCmdType_ChangePeer, raft_cmdpb.AdminCmdType_Split,
			raft_cmdpb.AdminCmdType_BatchSplit, raft_cmdpb.AdminCmdType_PrepareMerge,
			raft_cmdpb.AdminCmdType_CommitMerge, raft_cmdpb.AdminCmdType_RollbackMerge:
			return true
		default:
			return false
		}
	}
	req.Header.GetSyncLog()
}

type ReadExecutor struct {
	checkEpoch bool
	engine *DBBundle
	snapshot *DBSnapshot
	snapshotTime *time.Time
	needSnapshotTime bool
}

func NewReadExecutor(engine *DBBundle, checkEpoch bool, needSnapshotTime bool) *ReadExecutor {
	return &ReadExecutor{
		checkEpoch: checkEpoch,
		engine: engine,
		snapshot: nil,
		snapshotTime: nil,
		needSnapshotTime: needSnapshotTime,
	}
}

func (r *ReadExecutor) SnapshotTime() *time.Time {
	r.MaybeUpdateSnapshot()
	r.snapshotTime
}

func (r *ReadExecutor) MaybeUpdateSnapshot() {
	if r.snapshot != nil {
		return
	}
	r.snapshot = &DBSnapshot{
		Txn: r.engine.db.NewTransaction(false),
		LockStore: r.engine.lockStore,
		RollbackStore: r.engine.rollbackStore,
	}
	// Reading current timespec after snapshot, in case we do not
	// expire lease in time.
	// atomic::fence(atomic::Ordering::Release)
	if r.needSnapshotTime {
		t := time.Now()
		r.snapshotTime = &t
	}
}

func (r *ReadExecutor) DoGet(req *)

type Proposal struct {
	IsConfChange bool
	Index uint64
	Term uint64
	Cb Callback
}

type ProposalMeta struct {
	Index uint64
	Term uint64
	RenewLeaseTime *time.Time
}

type Apply struct {
	RegionId uint64
	Term uint64
	Entries []eraftpb.Entry
}

type ApplyMetrics struct {
	SizeDiffHint uint64
	DeleteKeysHint uint64
	WrittenBytes uint64
	WrittenKeys uint64
	LockCfWrittenBytes uint64
}

func (p *Peer) findProposeTime(index, term uint64) *time.Time {
	for {
		meta := p.proposals.Pop(term)
		if meta == nil {
			return nil
		}
		if meta.Index == index && meta.Term == term {
			return meta.RenewLeaseTime
		}
	}
	return nil
}

func (p *Peer) Term() uint64 {
	return p.RaftGroup.Raft.Term
}

func (p *Peer) GerPeerFromCache(id uint64) *metapb.Peer


