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
	"encoding/hex"
	"github.com/ngaut/unistore/tikv/raftstore/raftlog"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/memtable"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
)

type taskType int64

const (
	taskTypeStop           taskType = 0
	taskTypeRaftLogGC      taskType = 1
	taskTypeSplitCheck     taskType = 2
	taskTypeComputeHash    taskType = 3
	taskTypeHalfSplitCheck taskType = 4

	taskTypePDAskSplit         taskType = 101
	taskTypePDAskBatchSplit    taskType = 102
	taskTypePDHeartbeat        taskType = 103
	taskTypePDStoreHeartbeat   taskType = 104
	taskTypePDReportBatchSplit taskType = 105
	taskTypePDValidatePeer     taskType = 106
	taskTypePDReadStats        taskType = 107
	taskTypePDDestroyPeer      taskType = 108

	taskTypeCompact         taskType = 201
	taskTypeCheckAndCompact taskType = 202

	taskTypeRegionGen   taskType = 401
	taskTypeRegionApply taskType = 402
	/// Destroy data between [start_key, end_key).
	///
	/// The deletion may and may not succeed.
	taskTypeRegionDestroy           taskType = 403
	taskTypeRegionFollowerChangeSet taskType = 404

	taskTypeResolveAddr taskType = 501

	taskTypeSnapSend taskType = 601
	taskTypeSnapRecv taskType = 602
)

type task struct {
	tp   taskType
	data interface{}
}

type regionTask struct {
	region   *metapb.Region
	notifier chan<- *eraftpb.Snapshot
	status   *JobStatus
	snapData *snapData
	startKey []byte
	endKey   []byte
	redoIdx  uint64
}

type raftLogGCTask struct {
	raftEngine *badger.DB
	regionID   uint64
	startIdx   uint64
	endIdx     uint64
}

type splitCheckTask struct {
	region *metapb.Region
	peer   *metapb.Peer
}

type computeHashTask struct {
	index  uint64
	region *metapb.Region
	snap   *mvcc.DBSnapshot
}

type pdAskSplitTask struct {
	region   *metapb.Region
	splitKey []byte
	peer     *metapb.Peer
	// If true, right Region derives origin region_id.
	rightDerive bool
	callback    *Callback
}

type pdAskBatchSplitTask struct {
	region    *metapb.Region
	splitKeys [][]byte
	peer      *metapb.Peer
	// If true, right Region derives origin region_id.
	rightDerive bool
	callback    *Callback
}

type pdRegionHeartbeatTask struct {
	region          *metapb.Region
	peer            *metapb.Peer
	downPeers       []*pdpb.PeerStats
	pendingPeers    []*metapb.Peer
	writtenBytes    uint64
	writtenKeys     uint64
	approximateSize *uint64
	approximateKeys *uint64
}

type pdStoreHeartbeatTask struct {
	stats    *pdpb.StoreStats
	engine   *badger.ShardingDB
	path     string
	capacity uint64
}

type pdReportBatchSplitTask struct {
	regions []*metapb.Region
}

type pdValidatePeerTask struct {
	region      *metapb.Region
	peer        *metapb.Peer
	mergeSource *uint64
}

type readStats map[uint64]flowStats

type pdDestroyPeerTask struct {
	regionID uint64
}

type flowStats struct {
	readBytes uint64
	readKeys  uint64
}

type sendSnapTask struct {
	storeID  uint64
	msg      *raft_serverpb.RaftMessage
	callback func(error)
}

type recvSnapTask struct {
	stream   tikvpb.Tikv_SnapshotServer
	callback func(error)
}

type worker struct {
	name     string
	sender   chan<- task
	receiver <-chan task
	closeCh  chan struct{}
	wg       *sync.WaitGroup
}

type taskHandler interface {
	handle(t task)
}

type starter interface {
	start()
}

func (w *worker) start(handler taskHandler) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if s, ok := handler.(starter); ok {
			s.start()
		}
		for {
			task := <-w.receiver
			if task.tp == taskTypeStop {
				return
			}
			handler.handle(task)
		}
	}()
}

func (w *worker) stop() {
	w.sender <- task{tp: taskTypeStop}
}

const defaultWorkerCapacity = 128

func newWorker(name string, wg *sync.WaitGroup) *worker {
	ch := make(chan task, defaultWorkerCapacity)
	return &worker{
		sender:   (chan<- task)(ch),
		receiver: (<-chan task)(ch),
		name:     name,
		wg:       wg,
	}
}

type splitCheckHandler struct {
	engine *badger.ShardingDB
	router *router
	config *splitCheckConfig
}

func newSplitCheckRunner(engine *badger.ShardingDB, router *router, config *splitCheckConfig) *splitCheckHandler {
	runner := &splitCheckHandler{
		engine: engine,
		router: router,
		config: config,
	}
	return runner
}

/// run checks a region with split checkers to produce split keys and generates split admin command.
func (r *splitCheckHandler) handle(t task) {
	spCheckTask := t.data.(*splitCheckTask)
	region := spCheckTask.region
	regionId := region.Id
	_, startKey, err := codec.DecodeBytes(region.StartKey, nil)
	if err != nil {
		log.S().Errorf("failed to decode region key %x, err:%v", region.StartKey, err)
		return
	}
	_, endKey, err := codec.DecodeBytes(region.EndKey, nil)
	if err != nil {
		log.S().Errorf("failed to decode region key %x, err:%v", region.EndKey, err)
		return
	}
	log.S().Debugf("executing split check task: [regionId: %d, startKey: %s, endKey: %s]", regionId,
		hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	keys := r.engine.GetSplitSuggestion(regionId, int64(r.config.regionSplitSize))
	if len(keys) != 0 {
		_, err = splitEngineAndRegion(r.router, r.engine, spCheckTask.peer, region, keys)
		if err != nil {
			log.Warn("failed to send check result", zap.Uint64("region id", regionId), zap.Error(err))
		}
	} else {
		log.Debug("no need to send, split key not found", zap.Uint64("region id", regionId))
	}
}

// splitEngineAndRegion execute the complete procedure to split a region.
// 1. execute PreSplit on raft command.
// 2. Split the engine files.
// 3. Split the region.
func splitEngineAndRegion(router *router, engine *badger.ShardingDB, peer *metapb.Peer, region *metapb.Region, rawKeys [][]byte) ([]*metapb.Region, error) {
	header := raftlog.CustomHeader{
		RegionID: region.Id,
		Epoch:    raftlog.NewEpoch(region.RegionEpoch.Version, region.RegionEpoch.ConfVer),
		PeerID:   peer.Id,
		StoreID:  peer.StoreId,
	}
	builder := raftlog.NewBuilder(header)
	for _, k := range rawKeys {
		builder.AppendKeyOnly(k)
	}
	builder.SetType(raftlog.TypePreSplit)
	cb := NewCallback()
	cmd := &MsgRaftCmd{
		SendTime: time.Now(),
		Callback: cb,
		Request:  builder.Build(),
	}
	err := router.sendRaftCommand(cmd)
	if err != nil {
		return nil, err
	}
	cb.wg.Wait()
	if cb.resp.GetHeader().GetError() != nil {
		return nil, errors.New(cb.resp.Header.Error.Message)
	}
	err = engine.SplitShardFiles(region.Id, region.RegionEpoch.Version)
	if err != nil {
		return nil, err
	}
	encodedKeys := make([][]byte, len(rawKeys))
	for i := 0; i < len(rawKeys); i++ {
		encodedKeys[i] = codec.EncodeBytes(nil, rawKeys[i])
	}
	splitRegionCB := NewCallback()
	splitRegionMsg := &MsgSplitRegion{
		RegionEpoch: region.RegionEpoch,
		SplitKeys:   encodedKeys,
		Callback:    splitRegionCB,
	}
	err = router.send(region.Id, Msg{Type: MsgTypeSplitRegion, Data: splitRegionMsg})
	if err != nil {
		return nil, err
	}
	splitRegionCB.wg.Wait()
	return splitRegionCB.resp.GetAdminResponse().GetSplits().GetRegions(), nil
}

type stalePeerInfo struct {
	data []byte
}

func newStalePeerInfo(regionId uint64, endKey []byte, timeout time.Time) stalePeerInfo {
	s := stalePeerInfo{data: make([]byte, 16+len(endKey))}
	s.setRegionId(regionId)
	s.setTimeout(timeout)
	s.setEndKey(endKey)
	return s
}

func (s stalePeerInfo) regionId() uint64 {
	return binary.LittleEndian.Uint64(s.data[:8])
}

func (s stalePeerInfo) timeout() time.Time {
	return time.Unix(0, int64(binary.LittleEndian.Uint64(s.data[8:16])))
}

func (s stalePeerInfo) endKey() []byte {
	return s.data[16:]
}

func (s stalePeerInfo) setRegionId(regionId uint64) {
	binary.LittleEndian.PutUint64(s.data[:8], regionId)
}

func (s stalePeerInfo) setTimeout(timeout time.Time) {
	binary.LittleEndian.PutUint64(s.data[8:16], uint64(timeout.UnixNano()))
}

func (s stalePeerInfo) setEndKey(endKey []byte) {
	copy(s.data[16:], endKey)
}

type snapContext struct {
	engines   *Engines
	wb        *RaftWriteBatch
	batchSize uint64
}

// handleGen handles the task of generating snapshot of the Region. It calls `generateSnap` to do the actual work.
func (snapCtx *snapContext) handleGen(task *regionTask) {
	kv := snapCtx.engines.kv
	shard := kv.GetShard(task.region.Id)
	if shard.Ver != task.region.RegionEpoch.Version {
		log.Error("failed to generate snapshot, version not match")
		task.notifier <- new(eraftpb.Snapshot)
		return
	}
	changeSet := kv.GetShardChangeSet(task.region.Id)
	if changeSet.Version != shard.Ver {
		log.Error("failed to generate snapshot, version not match")
		task.notifier <- new(eraftpb.Snapshot)
	}
	propertyVals, dbSnap := kv.GetPropertiesWithSnap(shard, []string{applyStateKey})
	defer dbSnap.Discard()
	var deltaEntries deltaEntries
	for cf := 0; cf < kv.NumCFs(); cf++ {
		iter := dbSnap.NewDeltaIterator(cf, changeSet.Snapshot.CommitTS)
		for iter.Rewind(); iter.Valid(); iter.Next() {
			item := iter.Item()
			val, _ := item.ValueCopy(nil)
			deltaEntry := deltaEntry{
				cf:       cf,
				key:      item.Key(),
				val:      val,
				userMeta: item.UserMeta(),
				version:  item.Version(),
			}
			deltaEntries.encodeEntry(deltaEntry)
		}
	}
	applyState := new(applyState)
	applyState.Unmarshal(propertyVals[0])
	snapData := &snapData{
		region:       task.region,
		applyState:   applyState,
		changeSet:    changeSet,
		deltaEntries: deltaEntries,
		maxReadTS:    dbSnap.GetReadTS(),
	}
	snap := &eraftpb.Snapshot{
		Metadata: &eraftpb.SnapshotMetadata{},
		Data:     snapData.Marshal(),
	}
	snap.Metadata.Index = applyState.appliedIndex
	snap.Metadata.Term = applyState.appliedIndexTerm
	confState := confStateFromRegion(task.region)
	snap.Metadata.ConfState = &confState
	task.notifier <- snap
}

type regionTaskHandler struct {
	ctx *snapContext

	conf *config.Config
}

func newRegionTaskHandler(conf *config.Config, engines *Engines, batchSize uint64) *regionTaskHandler {
	return &regionTaskHandler{
		conf: conf,
		ctx: &snapContext{
			engines:   engines,
			batchSize: batchSize,
		},
	}
}

// handlePendingApplies tries to apply pending tasks if there is some.
func (r *regionTaskHandler) handleApply(task *regionTask) {
	atomic.StoreUint32(task.status, JobStatus_Running)
	r.ctx.wb = new(RaftWriteBatch)
	snapData := task.snapData
	db := r.ctx.engines.kv
	delta := snapData.deltaEntries
	cfTable := memtable.NewCFTable(r.conf.Engine.MaxMemTableSize, db.NumCFs())
	for len(delta.data) > 0 {
		entry := delta.decodeEntry()
		cfTable.Put(entry.cf, entry.key, y.ValueStruct{Value: entry.val, UserMeta: entry.userMeta, Version: entry.version})
	}
	inTree := &badger.IngestTree{
		ChangeSet: snapData.changeSet,
		MaxTS:     snapData.maxReadTS,
		Delta:     []*memtable.CFTable{cfTable},
	}
	err := r.ctx.engines.kv.Ingest(inTree)
	if err != nil {
		log.Error("update region status failed", zap.Error(err))
		atomic.StoreUint32(task.status, JobStatus_Failed)
	} else {
		atomic.StoreUint32(task.status, JobStatus_Finished)
	}
}

func (r *regionTaskHandler) handle(t task) {
	switch t.tp {
	case taskTypeRegionGen:
		// It is safe for now to handle generating and applying snapshot concurrently,
		// but it may not when merge is implemented.
		regionTask := t.data.(*regionTask)
		r.ctx.handleGen(regionTask)
	case taskTypeRegionApply:
		// To make sure applying snapshots in order.
		task := t.data.(*regionTask)
		r.handleApply(task)
	case taskTypeRegionDestroy:
		// We don't need to delay the range deletion because DeleteRange operation
		// doesn't affect the existing badger.Snapshot
		regionTask := t.data.(*regionTask)
		err := r.ctx.engines.kv.RemoveShard(regionTask.region.Id, true)
		if err != nil {
			log.Error("failed to destroy region", zap.Error(err))
		}
	case taskTypeRegionFollowerChangeSet:
		changeSet := t.data.(*protos.ShardChangeSet)
		kv := r.ctx.engines.kv
		shd := kv.GetShard(changeSet.ShardID)
		y.Assert(shd.Ver == changeSet.Version)
		err := kv.ApplySlowPassiveChangeSet(shd, changeSet)
		if err != nil {
			log.Error("failed to apply passive change set")
		}
	}
}

func (r *regionTaskHandler) shutdown() {
	// todo, currently it is a a place holder.
}

type raftLogGcTaskRes uint64

type raftLogGCTaskHandler struct {
	taskResCh chan<- raftLogGcTaskRes
}

// In our tests, we found that if the batch size is too large, running deleteAllInRange will
// reduce OLTP QPS by 30% ~ 60%. We found that 32K is a proper choice.
const MaxDeleteBatchSize int = 32 * 1024

// gcRaftLog does the GC job and returns the count of logs collected.
func (r *raftLogGCTaskHandler) gcRaftLog(raftDb *badger.DB, regionId, startIdx, endIdx uint64) (uint64, error) {

	// Find the raft log idx range needed to be gc.
	firstIdx := startIdx
	if firstIdx == 0 {
		firstIdx = endIdx
		err := raftDb.View(func(txn *badger.Txn) error {
			startKey := RaftLogKey(regionId, 0)
			ite := txn.NewIterator(badger.DefaultIteratorOptions)
			defer ite.Close()
			if ite.Seek(startKey); ite.Valid() {
				var err error
				if firstIdx, err = RaftLogIndex(ite.Item().Key()); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}

	if firstIdx >= endIdx {
		log.Info("no need to gc", zap.Uint64("region id", regionId))
		return 0, nil
	}

	raftWb := RaftWriteBatch{}
	for idx := firstIdx; idx < endIdx; idx += 1 {
		key := y.KeyWithTs(RaftLogKey(regionId, idx), RaftTS)
		raftWb.Delete(key)
		if raftWb.size >= MaxDeleteBatchSize {
			// Avoid large write batch to reduce latency.
			if err := raftWb.WriteToRaft(raftDb); err != nil {
				return 0, err
			}
			raftWb.Reset()
		}
	}
	// todo, disable WAL here.
	if raftWb.Len() != 0 {
		if err := raftWb.WriteToRaft(raftDb); err != nil {
			return 0, err
		}
	}
	return endIdx - firstIdx, nil
}

func (r *raftLogGCTaskHandler) reportCollected(collected uint64) {
	if r.taskResCh == nil {
		return
	}
	r.taskResCh <- raftLogGcTaskRes(collected)
}

func (r *raftLogGCTaskHandler) handle(t task) {
	logGcTask := t.data.(*raftLogGCTask)
	log.Debug("execute gc log", zap.Uint64("region id", logGcTask.regionID), zap.Uint64("end index", logGcTask.endIdx))
	collected, err := r.gcRaftLog(logGcTask.raftEngine, logGcTask.regionID, logGcTask.startIdx, logGcTask.endIdx)
	if err != nil {
		log.Error("failed to gc", zap.Uint64("region id", logGcTask.regionID), zap.Uint64("collected", collected), zap.Error(err))
	} else {
		log.Debug("collected log entries", zap.Uint64("region id", logGcTask.regionID), zap.Uint64("count", collected))
	}
	r.reportCollected(collected)
}

type compactTaskHandler struct {
	engine *badger.ShardingDB
}

func (r *compactTaskHandler) handle(t task) {
	// TODO: stub
}

type computeHashTaskHandler struct {
	router *router
}

func (r *computeHashTaskHandler) handle(t task) {
	// TODO: stub
}
