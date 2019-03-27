package raftstore

import (
	"github.com/ngaut/unistore/util"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/uber-go/atomic"
)

type pendingCmd struct {
	index uint64
	term  uint64
	cb    Callback
}

type pendingCmdQueue struct {
	normals    []pendingCmd
	confChange *pendingCmd
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

type ApplyTask struct {
	RegionId uint64
	Term     uint64
	Entries  []eraftpb.Entry
}

type ApplyMetrics struct {
	SizeDiffHint       uint64
	DeleteKeysHint     uint64
	WrittenBytes       uint64
	WrittenKeys        uint64
	LockCfWrittenBytes uint64
}

type ApplyTaskRes struct {
	regionID         uint64
	applyState       rspb.RaftApplyState
	appliedIndexTerm uint64
	execResults      []execResult
	metrics          *ApplyMetrics
	merged           bool

	destroyPeerID uint64
}

type execResultChangePeer struct {
	cp changePeer
}

type execResultCompactLog struct {
	state      *rspb.RaftTruncatedState
	firstIndex uint64
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
	snap   *DBSnapshot
}

type execResultVerifyHash struct {
	index uint64
	hash  []byte
}

type execResultDeleteRange struct {
	ranges []keyRange
}

type execResultIngestSST struct {
	ssts []*import_sstpb.SSTMeta
}

type execResult struct {
	data interface{}
}

type applyResultType int

const (
	None applyResultType = iota
	ExecResultType
	WaitMergeResourceType
)

/// applyResult has three kinds of types which are `None`, `ExecResultType` and `WaitMergeResourceType`.
/// The data is different for `ExecResultType` and `WaitMergeResourceType`. For `ExecResultType`, the data
/// is a ExecResult, and for `WaitMergeResourceType`, it is a `WaitMergeSource`.
type applyResult struct {
	tp   applyResultType
	data interface{}
}

type waitMergeSource struct {
	status *atomic.Uint64
}

type applyExecContext struct {
	index      uint64
	term       uint64
	applyState *rspb.RaftApplyState
}

type applyCallback struct {
	region *metapb.Region
	cbs    []*callBackResponseHolder
}

type callBackResponseHolder struct {
	callBack        Callback
	raftCmdResponse *raft_cmdpb.RaftCmdResponse
}

type proposal struct {
	isConfChange bool
	index        uint64
	term         uint64
	Cb           Callback
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
	id               uint64
	term             uint64
	applyState       *rspb.RaftApplyState
	appliedIndexTerm uint64
	region           *metapb.Region
}

type applyRouter struct {
	router
}

func (r *applyRouter) scheduleTask(regionId uint64, msg Msg) {
	// TODO: stub
}

type notifier struct {
	router *router
}

type applyContext struct {
	tag              string
	timer            *util.SlowTimer
	host             *CoprocessorHost
	router           applyRouter
	notifier         notifier
	engines          *Engines
	cbs              util.MustConsumerVec
	applyTaskResList []ApplyTaskRes
	execCtx          *applyExecContext
	wb               *WriteBatch
	wbLastBytes      uint64
	wbLastKeys       uint64
	lastAppliedIndex uint64
	committedCount   uint64

	enableSyncLog  bool
	syncLogHint    bool
	useDeleteRange bool
}

type waitSourceMergeState struct {
	pendingEntries []*eraftpb.Entry
	pendingMsgs    []Msg
	readyToMerge   *atomic.Uint64
	catchUpLogs    *catchUpLogs
}

type applyDelegate struct {
	id                uint64
	term              uint64
	region            *metapb.Region
	tag               string
	stopped           bool
	pendingRemove     bool
	pendingCmds       pendingCmdQueue
	merged            bool
	isMerging         bool
	lastMergeVersion  uint64
	waitMergeState    *waitSourceMergeState
	readySourceRegion uint64
	applyState        *rspb.RaftApplyState
	appliedIndexTerm  uint64
	metrics           ApplyMetrics
}

type applyDestroy struct {
	regionId uint64
}

type catchUpLogs struct {
	targetMailBox mailbox
	merge         *raft_cmdpb.CommitMergeRequest
	readyToMerge  *atomic.Uint64
}

type applyPollerBuilder struct {
	tag             string
	cfg             *Config
	coprocessorHost *CoprocessorHost
	engines         *Engines
	sender          notifier
	router          applyRouter
}

func newApplyPollerBuilder(raftPollerBuilder *raftPollerBuilder, sender notifier, router *applyRouter) *applyPollerBuilder {
	return nil // TODO: stub
}

func (b *applyPollerBuilder) build() pollHandler {
	return nil // TODO: stub
}

type applyFsm struct {
}

func newApplyFsmFromPeer(peer *peerFsm) (chan<- Msg, fsm) {
	return nil, nil // TODO: stub
}
