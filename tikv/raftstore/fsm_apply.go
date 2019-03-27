package raftstore

import (
	"container/list"
	"github.com/ngaut/unistore/tikv/import"
	"github.com/ngaut/unistore/util"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/uber-go/atomic"
)

type PendingCmd struct {
	index uint64
	term  uint64
	cb    Callback
}

type PendingCmdQueue struct {
	normals    *list.List
	confChange *PendingCmd
}

func (pendQueue *PendingCmdQueue) popNormal(term uint64) *PendingCmd {
	ele := pendQueue.normals.Front()
	if ele != nil {
		cmd := ele.Value.(*PendingCmd)
		if cmd.term > term {
			return nil
		}
		pendQueue.normals.Remove(ele)
		return cmd
	}
	return nil
}

func (pendQueue *PendingCmdQueue) appendNormal(cmd *PendingCmd) {
	pendQueue.normals.PushBack(cmd)
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

type ApplyResultType int

const (
	None ApplyResultType = iota
	ExecResultType
	WaitMergeResourceType
)

/// applyResult has three kinds of types which are `None`, `ExecResultType` and `WaitMergeResourceType`.
/// The data is different for `ExecResultType` and `WaitMergeResourceType`. For `ExecResultType`, the data
/// is a ExecResult, and for `WaitMergeResourceType`, it is a `WaitMergeSource`.
type ApplyResult struct {
	tp   ApplyResultType
	data interface{}
}

type WaitMergeSource struct {
	status *atomic.Uint64
}

type ExecContext struct {
	index      uint64
	term       uint64
	applyState rspb.RaftApplyState
}

type ApplyCallback struct {
	region metapb.Region
	cbs    []*CallBackResponseHolder
}

type CallBackResponseHolder struct {
	callBack        Callback
	raftCmdResponse raft_cmdpb.RaftCmdResponse
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

type ApplyContext struct {
	tag              string
	timer            *util.SlowTimer
	host             *CoprocessorHost
	importer         *_import.SSTImporter
	router           ApplyRouter
	notifier         notifier
	engines          Engines
	cbs              util.MustConsumerVec
	applyTaskResList []ApplyTaskRes
	execCtx          *ExecContext
	wb               *WriteBatch
	wbLastBytes      uint64
	wbLastKeys       uint64
	lastAppliedIndex uint64
	committedCount   uint64

	enableSyncLog  bool
	syncLogHint    bool
	useDeleteRange bool
}

type WaitSourceMergeState struct {
	pendingEntries []eraftpb.Entry
	pendingMsgs    []Msg
	readyToMerge   *atomic.Uint64
	catchUpLogs    *CatchUpLogs
}

type ApplyDelegate struct {
	id                uint64
	term              uint64
	region            metapb.Region
	tag               string
	stopped           bool
	pendingRemove     bool
	pendingCmds       PendingCmdQueue
	merged            bool
	isMerging         bool
	lastMergeVersion  uint64
	waitMergeState    *WaitSourceMergeState
	readySourceRegion uint64
	applyState        rspb.RaftApplyState
	appliedIndexTerm  uint64
	metrics           ApplyMetrics
}

type Destroy struct {
	regionId uint64
	peerId   uint64
}

type DelegateMailBox mailbox

type CatchUpLogs struct {
	targetMailBox DelegateMailBox
	merge         raft_cmdpb.CommitMergeRequest
	readyToMerge  *atomic.Uint64
}

type TaskResType int

const (
	ApplyTaskResType TaskResType = iota
	DestroyResType
)

type TaskRes struct {
	tp      TaskResType
	destroy Destroy
}

type Builder struct {
	tag             string
	cfg             *Config
	coprocessorHost *CoprocessorHost
	importer        *_import.SSTImporter
	engines         Engines
	sender          notifier
	router          ApplyRouter
}

type ApplyBatchSystem batchSystem

type ApplyRouter struct {
	// Todo: currently it is a place holder
}

func (a *ApplyRouter) ScheduleTask(regionId uint64, msg Msg) {
	// Todo: currently it is a place holder
}

type applyPollerBuilder struct {
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
