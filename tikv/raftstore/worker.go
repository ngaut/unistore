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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/pingcap/badger/table/memtable"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
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
	taskTypeRegionDestroy taskType = 403

	taskTypeResolveAddr taskType = 501

	taskTypeSnapSend taskType = 601
	taskTypeSnapRecv taskType = 602
)

type task struct {
	tp   taskType
	data interface{}
}

type regionTask struct {
	regionId uint64
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

type compactTask struct {
	keyRange keyRange
}

type checkAndCompactTask struct {
	ranges                    []keyRange
	tombStoneNumThreshold     uint64 // The minimum RocksDB tombstones a range that need compacting has
	tombStonePercentThreshold uint64
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
	engine   *badger.ShardingDB
	router   *router
	config   *splitCheckConfig
	checkers []splitChecker
}

func newSplitCheckRunner(engine *badger.ShardingDB, router *router, config *splitCheckConfig) *splitCheckHandler {
	runner := &splitCheckHandler{
		engine: engine,
		router: router,
		config: config,
	}
	return runner
}

func (r *splitCheckHandler) newCheckers() {
	r.checkers = r.checkers[:0]
	// the checker append order is the priority order
	sizeChecker := newSizeSplitChecker(r.config.regionMaxSize, r.config.regionSplitSize, r.config.batchSplitLimit)
	r.checkers = append(r.checkers, sizeChecker)
	keysChecker := newKeysSplitChecker(r.config.RegionMaxKeys, r.config.RegionSplitKeys, r.config.batchSplitLimit)
	r.checkers = append(r.checkers, keysChecker)
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
	snap := r.engine.NewSnapshot(startKey, endKey)
	reader := dbreader.NewDBReader(startKey, endKey, snap)
	defer reader.Close()
	var keys [][]byte
	switch t.tp {
	case taskTypeHalfSplitCheck:
		keys = r.halfSplitCheck(startKey, endKey, reader)
	case taskTypeSplitCheck:
		keys = r.splitCheck(startKey, endKey, reader)
	}
	if len(keys) != 0 {
		regionEpoch := region.GetRegionEpoch()
		for i, k := range keys {
			keys[i] = codec.EncodeBytes(nil, k)
		}
		msg := Msg{
			Type:     MsgTypeSplitRegion,
			RegionID: regionId,
			Data: &MsgSplitRegion{
				RegionEpoch: regionEpoch,
				SplitKeys:   keys,
				Callback:    NewCallback(),
			},
		}
		err = r.router.send(regionId, msg)
		if err != nil {
			log.Warn("failed to send check result", zap.Uint64("region id", regionId), zap.Error(err))
		}
	} else {
		log.Debug("no need to send, split key not found", zap.Uint64("region id", regionId))
	}
}

func exceedEndKey(current, endKey []byte) bool {
	return bytes.Compare(current, endKey) >= 0
}

// doCheck checks kvs using every checker
func (r *splitCheckHandler) doCheck(startKey, endKey []byte, ite *badger.Iterator) {
	r.newCheckers()
	for ite.Seek(startKey); ite.Valid(); ite.Next() {
		item := ite.Item()
		key := item.Key()
		if exceedEndKey(key, endKey) {
			break
		}
		for _, checker := range r.checkers {
			if checker.onKv(key, item) {
				return
			}
		}
	}
}

/// SplitCheck gets the split keys by scanning the range.
func (r *splitCheckHandler) splitCheck(startKey, endKey []byte, reader *dbreader.DBReader) [][]byte {
	ite := reader.GetIter()
	splitKeys := r.tryTableSplit(startKey, endKey, ite)
	if len(splitKeys) > 0 {
		return splitKeys
	}
	r.doCheck(startKey, endKey, ite)
	for _, checker := range r.checkers {
		keys := checker.getSplitKeys()
		if len(keys) > 0 {
			return keys
		}
	}
	return nil
}

func (r *splitCheckHandler) tryTableSplit(startKey, endKey []byte, it *badger.Iterator) [][]byte {
	if !isTableKey(startKey) || isSameTable(startKey, endKey) {
		return nil
	}
	var splitKeys [][]byte
	prevKey := startKey
	for {
		it.Seek(nextTableKey(prevKey))
		if !it.Valid() {
			break
		}
		key := it.Item().Key()
		if exceedEndKey(key, endKey) {
			break
		}
		splitKey := safeCopy(key)
		splitKeys = append(splitKeys, splitKey)
		prevKey = splitKey
	}
	return splitKeys
}

func nextTableKey(key []byte) []byte {
	result := make([]byte, 9)
	result[0] = 't'
	if len(key) >= 9 {
		curTableID := binary.BigEndian.Uint64(key[1:])
		binary.BigEndian.PutUint64(result[1:], curTableID+1)
	}
	return result
}

type splitChecker interface {
	onKv(key []byte, item *badger.Item) bool
	getSplitKeys() [][]byte
}

type sizeSplitChecker struct {
	maxSize         uint64
	splitSize       uint64
	currentSize     uint64
	splitKeys       [][]byte
	batchSplitLimit uint64
}

func newSizeSplitChecker(maxSize, splitSize, batchSplitLimit uint64) *sizeSplitChecker {
	return &sizeSplitChecker{
		maxSize:         maxSize,
		splitSize:       splitSize,
		batchSplitLimit: batchSplitLimit,
	}
}

func safeCopy(b []byte) []byte {
	return append([]byte{}, b...)
}

func (checker *sizeSplitChecker) onKv(key []byte, item *badger.Item) bool {
	valueSize := uint64(item.ValueSize())
	size := uint64(len(key)) + valueSize
	checker.currentSize += size
	overLimit := uint64(len(checker.splitKeys)) >= checker.batchSplitLimit
	if checker.currentSize > checker.splitSize && !overLimit {
		checker.splitKeys = append(checker.splitKeys, safeCopy(key))
		// If for previous onKv(), checker.current_size == checker.split_size,
		// the split key would be pushed this time, but the entry size for this time should not be ignored.
		if checker.currentSize-size == checker.splitSize {
			checker.currentSize = size
		} else {
			checker.currentSize = 0
		}
		overLimit = uint64(len(checker.splitKeys)) >= checker.batchSplitLimit
	}
	// For a large region, scan over the range maybe cost too much time,
	// so limit the number of produced splitKeys for one batch.
	// Also need to scan over checker.maxSize for last part.
	return overLimit && checker.currentSize+checker.splitSize >= checker.maxSize
}

func (checker *sizeSplitChecker) getSplitKeys() [][]byte {
	// Make sure not to split when less than maxSize for last part
	if checker.currentSize+checker.splitSize < checker.maxSize {
		splitKeyLen := len(checker.splitKeys)
		if splitKeyLen != 0 {
			checker.splitKeys = checker.splitKeys[:splitKeyLen-1]
		}
	}
	keys := checker.splitKeys
	checker.splitKeys = nil
	return keys
}

func newKeysSplitChecker(regionMaxKeys, regionSplitKeys, batchSplitLimit uint64) *keysSplitChecker {
	checker := &keysSplitChecker{
		regionMaxKeys:   regionMaxKeys,
		regionSplitKeys: regionSplitKeys,
		batchSplitLimit: batchSplitLimit,
	}
	return checker
}

type keysSplitChecker struct {
	regionMaxKeys   uint64
	regionSplitKeys uint64
	batchSplitLimit uint64
	curCnt          uint64
	splitKeys       [][]byte
}

func (c *keysSplitChecker) onKv(key []byte, item *badger.Item) bool {
	c.curCnt++
	overLimit := uint64(len(c.splitKeys)) >= c.batchSplitLimit
	if c.curCnt > c.regionSplitKeys && !overLimit {
		// if for previous on_kv() self.current_count == self.split_threshold,
		// the split key would be pushed this time, but the entry for this time should not be ignored.
		c.splitKeys = append(c.splitKeys, safeCopy(key))
		c.curCnt = 1
		overLimit = uint64(len(c.splitKeys)) >= c.batchSplitLimit
	}
	return overLimit && c.curCnt+c.regionSplitKeys >= c.regionMaxKeys
}

func (c *keysSplitChecker) getSplitKeys() [][]byte {
	// make sure not to split when less than max_keys_count for last part
	if c.curCnt+c.regionSplitKeys < c.regionMaxKeys {
		if len(c.splitKeys) > 0 {
			c.splitKeys = c.splitKeys[:len(c.splitKeys)-1]
		}
	}
	keys := c.splitKeys
	c.splitKeys = nil
	return keys
}

func (r *splitCheckHandler) halfSplitCheck(startKey, endKey []byte, reader *dbreader.DBReader) [][]byte {
	var sampleKeys [][]byte
	cnt := 0
	ite := reader.GetIter()
	for ite.Seek(startKey); ite.Valid(); ite.Next() {
		cnt++
		key := ite.Item().Key()
		if exceedEndKey(key, endKey) {
			break
		}
		if cnt%r.config.rowsPerSample == 0 {
			sampleKeys = append(sampleKeys, safeCopy(key))
		}
	}
	mid := len(sampleKeys) / 2
	if len(sampleKeys) > mid {
		splitKey := sampleKeys[mid]
		return [][]byte{splitKey}
	}
	return nil
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
	wb        *WriteBatch
	batchSize uint64
	mgr       *SnapManager
}

// handleGen handles the task of generating snapshot of the Region. It calls `generateSnap` to do the actual work.
func (snapCtx *snapContext) handleGen(task *regionTask) {
	snap, err := snapCtx.engines.metaManager.getSnapshotData(task.regionId)
	if err != nil {
		log.Error("failed to generate snapshot", zap.Error(err))
		return
	}
	task.notifier <- snap
}

// generateSnap generates the snapshots of the Region
func (snapCtx *snapContext) generateSnap(task *regionTask) error {
	// do we need to check leader here?
	snap, err := doSnapshot(snapCtx.engines, snapCtx.mgr, task)
	if err != nil {
		return err
	}
	task.notifier <- snap
	return nil
}

// cleanUpOriginData clear up the region data before applying snapshot
func (snapCtx *snapContext) cleanUpOriginData(regionState *rspb.RegionLocalState, status *JobStatus) error {
	startKey := RawStartKey(regionState.GetRegion())
	endKey := RawEndKey(regionState.GetRegion())
	if err := checkAbort(status); err != nil {
		return err
	}
	if err := snapCtx.engines.kv.DeleteRange(startKey, endKey); err != nil {
		return err
	}
	if err := checkAbort(status); err != nil {
		return err
	}
	return nil
}

// applySnap applies snapshot data of the Region.
func (snapCtx *snapContext) applySnap(regionId uint64, status *JobStatus, builder *sstable.Builder) (ApplyResult, error) {
	log.Info("begin apply snap data", zap.Uint64("region id", regionId))
	var result ApplyResult
	if err := checkAbort(status); err != nil {
		return result, err
	}

	regionKey := RegionStateKey(regionId)
	regionState, err := getRegionLocalState(snapCtx.engines.kv, regionId)
	if err != nil {
		return result, errors.New(fmt.Sprintf("failed to get regionState from %v", regionKey))
	}

	// Clean up origin data
	if err := snapCtx.cleanUpOriginData(regionState, status); err != nil {
		return result, err
	}

	applyState, err := getApplyState(snapCtx.engines.kv, regionId)
	if err != nil {
		return result, errors.New(fmt.Sprintf("failed to get raftState from %v", ApplyStateKey(regionId)))
	}
	snapKey := SnapKey{RegionID: regionId, Index: applyState.truncatedIndex, Term: applyState.truncatedTerm}
	snapCtx.mgr.Register(snapKey, SnapEntryApplying)
	defer snapCtx.mgr.Deregister(snapKey, SnapEntryApplying)

	snap, err := snapCtx.mgr.GetSnapshotForApplying(snapKey)
	if err != nil {
		return result, errors.New(fmt.Sprintf("missing snapshot file %s", snapKey))
	}

	t := time.Now()
	applyOptions := newApplyOptions(snapCtx.engines.kv, regionState.GetRegion(), status, builder, snapCtx.wb)
	if result, err = snap.Apply(*applyOptions); err != nil {
		return result, err
	}

	regionState.State = rspb.PeerState_Normal
	result.RegionState = regionState

	log.Info("applying new data", zap.Uint64("region id", regionId), zap.Duration("takes", time.Since(t)))
	return result, nil
}

// handleApply tries to apply the snapshot of the specified Region. It calls `applySnap` to do the actual work.
func (snapCtx *snapContext) handleApply(regionId uint64, status *JobStatus, builder *sstable.Builder) (ApplyResult, error) {
	atomic.CompareAndSwapUint32(status, JobStatus_Pending, JobStatus_Running)
	result, err := snapCtx.applySnap(regionId, status, builder)
	switch err.(type) {
	case nil:
		atomic.SwapUint32(status, JobStatus_Finished)
	case applySnapAbortError:
		log.Warn("applying snapshot is aborted", zap.Uint64("region id", regionId))
		y.Assert(atomic.SwapUint32(status, JobStatus_Cancelled) == JobStatus_Cancelling)
	default:
		log.Error("failed to apply snap!!!", zap.Error(err))
		atomic.SwapUint32(status, JobStatus_Failed)
	}
	return result, err
}

/// ingestMaybeStall checks the number of files at level 0 to avoid write stall after ingesting sst.
/// Returns true if the ingestion causes write stall.
func (snapCtx *snapContext) ingestMaybeStall() bool {
	for _, cf := range snapshotCFs {
		if plainFileUsed(cf) {
			continue
		}
		// todo, related to cf.
	}
	return false
}

type regionApplyState struct {
	localState *rspb.RegionLocalState
	tableCount int
}

type regionTaskHandler struct {
	ctx *snapContext

	conf *config.Config
}

func newRegionTaskHandler(conf *config.Config, engines *Engines, mgr *SnapManager, batchSize uint64) *regionTaskHandler {
	return &regionTaskHandler{
		conf: conf,
		ctx: &snapContext{
			engines:   engines,
			mgr:       mgr,
			batchSize: batchSize,
		},
	}
}

// handlePendingApplies tries to apply pending tasks if there is some.
func (r *regionTaskHandler) handleApply(task *regionTask) {
	atomic.StoreUint32(task.status, JobStatus_Running)
	r.ctx.wb = new(WriteBatch)
	snapData := task.snapData
	db := r.ctx.engines.kv
	delta := snapData.deltaEntries
	cfTable := memtable.NewCFTable(r.conf.Engine.MaxMemTableSize, db.NumCFs())
	for len(delta.data) > 0 {
		entry := delta.decodeEntry()
		cfTable.Put(entry.cf, entry.key, y.ValueStruct{Value: entry.val, UserMeta: entry.userMeta, Version: entry.version})
	}
	r.ctx.engines.metaManager.applySnapshot(task.snapData)
	wb := r.ctx.wb
	r.ctx.wb = nil
	rs := new(rspb.RegionLocalState)
	rs.Region = snapData.region
	regionID := rs.Region.Id
	rsVal, _ := rs.Marshal()
	wb.SetRaftCF(y.KeyWithTs(RegionStateKey(regionID), KvTS), rsVal)
	wb.SetRaftCF(y.KeyWithTs(SnapshotRaftStateKey(regionID), KvTS), nil)
	if err := wb.WriteToKV(r.ctx.engines.kv); err != nil {
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
		regionTask := t.data.(regionTask)
		err := r.ctx.engines.kv.DeleteRange(regionTask.startKey, regionTask.endKey)
		if err != nil {
			log.Error("failed to destroy region", zap.Error(err))
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

	raftWb := WriteBatch{}
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
