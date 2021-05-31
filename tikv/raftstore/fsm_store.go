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
	"github.com/ngaut/unistore/sdbpb"
	"github.com/ngaut/unistore/tikv/regiontree"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/tikv/raftstore/raftlog"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/log"
)

type storeMeta struct {
	/// region end key -> region ID
	regionTree *regiontree.RegionTree
	/// region_id -> region
	regions map[uint64]*metapb.Region
	/// `MsgRequestPreVote` or `MsgRequestVote` messages from newly split Regions shouldn't be dropped if there is no
	/// such Region in this store now. So the messages are recorded temporarily and will be handled later.
	pendingVotes []*rspb.RaftMessage
	/// A marker used to indicate the peer of a Region has received a merge target message and waits to be destroyed.
	/// target_region_id -> (source_region_id -> merge_target_epoch)
	pendingMergeTargets map[uint64]map[uint64]*metapb.RegionEpoch
	/// An inverse mapping of `pending_merge_targets` used to let source peer help target peer to clean up related entry.
	/// source_region_id -> target_region_id
	targetsMap map[uint64]uint64
	/// In raftstore, the execute order of `PrepareMerge` and `CommitMerge` is not certain because of the messages
	/// belongs two regions. To make them in order, `PrepareMerge` will set this structure and `CommitMerge` will retry
	/// later if there is no related lock.
	/// source_region_id -> (version, BiLock).
	mergeLocks map[uint64]*mergeLock
}

func newStoreMeta() *storeMeta {
	return &storeMeta{
		regionTree:          regiontree.NewRegionTree(),
		regions:             map[uint64]*metapb.Region{},
		pendingMergeTargets: map[uint64]map[uint64]*metapb.RegionEpoch{},
		targetsMap:          map[uint64]uint64{},
		mergeLocks:          map[uint64]*mergeLock{},
	}
}

func (m *storeMeta) setRegion(region *metapb.Region, peer *Peer) {
	m.regions[region.Id] = region
	peer.SetRegion(region)
}

type mergeLock struct {
}

type GlobalContext struct {
	cfg                   *Config
	engine                *Engines
	store                 *metapb.Store
	storeMeta             *storeMeta
	storeMetaLock         *sync.RWMutex
	router                *router
	trans                 *RaftClient
	pdTaskSender          chan<- task
	regionTaskSender      chan<- task
	computeHashTaskSender chan<- task
	raftLogGCTaskSender   chan<- task
	splitCheckTaskSender  chan<- task
	pdClient              pd.Client
	peerEventObserver     PeerEventObserver
	globalStats           *storeStats
}

type StoreContext struct {
	*GlobalContext
	applyingSnapCount *uint64
}

type RaftContext struct {
	*GlobalContext
	applyMsgs    *applyMsgs
	ReadyRes     []*ReadyICPair
	raftWB       *RaftWriteBatch
	pendingCount int
	hasReady     bool
	isBusy       bool
	localStats   *storeStats
}

type storeStats struct {
	engineTotalBytesWritten uint64
	engineTotalKeysWritten  uint64
	isBusy                  uint64
}

type Transport interface {
	Send(msg *rspb.RaftMessage)
}

func (pc *RaftContext) flushLocalStats() {
	if pc.localStats.engineTotalBytesWritten > 0 {
		atomic.AddUint64(&pc.globalStats.engineTotalBytesWritten, pc.localStats.engineTotalBytesWritten)
		pc.localStats.engineTotalBytesWritten = 0
	}
	if pc.localStats.engineTotalKeysWritten > 0 {
		atomic.AddUint64(&pc.globalStats.engineTotalKeysWritten, pc.localStats.engineTotalKeysWritten)
		pc.localStats.engineTotalKeysWritten = 0
	}
	if pc.localStats.isBusy > 0 {
		atomic.StoreUint64(&pc.globalStats.isBusy, pc.localStats.isBusy)
		pc.localStats.isBusy = 0
	}
}

type storeFsm struct {
	id                   uint64
	lastCompactCheckKey  []byte
	stopped              bool
	startTime            *time.Time
	consistencyCheckTime map[uint64]time.Time
	receiver             <-chan Msg
	ticker               *ticker
}

func newStoreFsm(cfg *Config) (chan<- Msg, *storeFsm) {
	ch := make(chan Msg, cfg.NotifyCapacity)
	fsm := &storeFsm{
		consistencyCheckTime: map[uint64]time.Time{},
		receiver:             (<-chan Msg)(ch),
		ticker:               newStoreTicker(cfg),
	}
	return (chan<- Msg)(ch), fsm
}

type storeMsgHandler struct {
	*storeFsm
	ctx *StoreContext
}

func newStoreFsmDelegate(store *storeFsm, ctx *StoreContext) *storeMsgHandler {
	return &storeMsgHandler{storeFsm: store, ctx: ctx}
}

func (d *storeMsgHandler) onTick(tick StoreTick) {
	switch tick {
	case StoreTickPdStoreHeartbeat:
		d.onPDStoreHearbeatTick()
	case StoreTickConsistencyCheck:
		d.onComputeHashTick()
	}
}

func (d *storeMsgHandler) handleMsg(msg Msg) {
	switch msg.Type {
	case MsgTypeStoreRaftMessage:
		if err := d.onRaftMessage(msg.Data.(*rspb.RaftMessage)); err != nil {
			log.S().Errorf("handle raft message failed storeID %d, %v", d.id, err)
		}
	case MsgTypeStoreTick:
		d.onTick(msg.Data.(StoreTick))
	case MsgTypeStoreStart:
		d.start(msg.Data.(*metapb.Store))
	case MsgTypeGenerateEngineChangeSet:
		d.onGenerateEngineMetaChange(msg)
	}
}

func (d *storeMsgHandler) start(store *metapb.Store) {
	if d.startTime != nil {
		panic(fmt.Sprintf("store %d unable to start again %s", d.id, store))
	}
	d.id = store.Id
	now := time.Now()
	d.startTime = &now
	d.ticker.scheduleStore(StoreTickPdStoreHeartbeat)
	d.ticker.scheduleStore(StoreTickConsistencyCheck)
}

/// loadPeers loads peers in this store. It scans the db engine, loads all regions
/// and their peers from it, and schedules snapshot worker if necessary.
/// WARN: This store should not be used before initialized.
func (bs *raftBatchSystem) loadPeers() ([]*peerFsm, error) {
	// Scan region meta to get saved regions.
	startKey := RegionMetaMinKey
	endKey := RegionMetaMaxKey
	ctx := bs.ctx
	raftEngine := ctx.engine.raft
	storeID := ctx.store.Id

	var totalCount, tombStoneCount, applyingCount int
	var regionPeers []*peerFsm

	t := time.Now()
	raftWB := new(RaftWriteBatch)
	var applyingRegions []*metapb.Region
	var mergingCount int
	ctx.storeMetaLock.Lock()
	defer ctx.storeMetaLock.Unlock()
	meta := ctx.storeMeta
	err := raftEngine.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.Reverse = true
		iter := txn.NewIterator(opt)
		defer iter.Close()
		var lastRegionID uint64
		for iter.Seek(endKey); iter.Valid(); iter.Next() {
			item := iter.Item()
			if bytes.Compare(item.Key(), startKey) <= 0 {
				break
			}
			regionID, _, _, err := decodeRegionMetaKey(item.Key())
			if err != nil {
				return err
			}
			if regionID == lastRegionID {
				continue
			}
			lastRegionID = regionID
			val, err := item.Value()
			if err != nil {
				return errors.WithStack(err)
			}
			totalCount++
			localState := new(rspb.RegionLocalState)
			err = localState.Unmarshal(val)
			if err != nil {
				return errors.WithStack(err)
			}
			region := localState.Region
			if localState.State == rspb.PeerState_Tombstone {
				tombStoneCount++
				bs.clearStaleMeta(raftWB, localState)
				continue
			}
			if localState.State == rspb.PeerState_Applying {
				// in case of restart happen when we just write region state to Applying,
				// but not write raft_local_state to raft rocksdb in time.
				applyingCount++
				applyingRegions = append(applyingRegions, region)
				continue
			}

			peer, err := createPeerFsm(storeID, ctx.cfg, ctx.regionTaskSender, ctx.engine, region)
			if err != nil {
				return err
			}
			shard := ctx.engine.kv.GetShard(regionID)
			peer.peer.Store().initialFlushed = shard.IsInitialFlushed()
			peer.peer.Store().splitStage = shard.GetSplitStage()
			ctx.peerEventObserver.OnPeerCreate(peer.peer.getEventContext(), region)
			if localState.State == rspb.PeerState_Merging {
				log.S().Infof("region %d is merging", regionID)
				mergingCount++
				peer.setPendingMergeState(localState.MergeState)
			}
			meta.regionTree.Put(region)
			meta.regions[regionID] = region
			// No need to check duplicated here, because we use region id as the key
			// in DB.
			regionPeers = append(regionPeers, peer)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if raftWB.size > 0 {
		raftWB.MustWriteToRaft(ctx.engine.raft)
	}

	// schedule applying snapshot after raft write batch were written.
	for _, region := range applyingRegions {
		log.S().Infof("region %d is applying snapshot", region.Id)
		peer, err := createPeerFsm(storeID, ctx.cfg, ctx.regionTaskSender, ctx.engine, region)
		if err != nil {
			return nil, err
		}
		status := JobStatus_Cancelled
		peer.peer.Store().snapState = SnapState{
			StateType: SnapState_ApplyAborted,
			Status:    &status,
		}
		meta.regionTree.Put(region)
		meta.regions[region.Id] = region
		regionPeers = append(regionPeers, peer)
	}
	log.S().Infof("start store %d, region_count %d, tombstone_count %d, applying_count %d, merge_count %d, takes %v",
		storeID, totalCount, tombStoneCount, applyingCount, mergingCount, time.Since(t))
	return regionPeers, nil
}

func (bs *raftBatchSystem) clearStaleMeta(raftWB *RaftWriteBatch, originState *rspb.RegionLocalState) {
	region := originState.Region
	raftKey := RaftStateKey(region)
	raftState := raftState{}
	val, err := getValue(bs.ctx.engine.raft, raftKey)
	if err != nil {
		// it has been cleaned up.
		return
	}
	raftState.Unmarshal(val)
	err = ClearMeta(bs.ctx.engine, raftWB, region, raftState.lastIndex)
	if err != nil {
		panic(err)
	}
}

type workers struct {
	pdWorker          *worker
	raftLogGCWorker   *worker
	computeHashWorker *worker
	splitCheckWorker  *worker
	regionWorker      *worker
	wg                *sync.WaitGroup
}

type raftBatchSystem struct {
	ctx       *GlobalContext
	router    *router
	workers   *workers
	closeCh   chan struct{}
	wg        *sync.WaitGroup
	globalCfg *config.Config
}

func (bs *raftBatchSystem) start(
	meta *metapb.Store,
	cfg *Config,
	engines *Engines,
	trans *RaftClient,
	pdClient pd.Client,
	pdWorker *worker,
	observer PeerEventObserver) error {
	y.Assert(bs.workers == nil)
	// TODO: we can get cluster meta regularly too later.
	if err := cfg.Validate(); err != nil {
		return err
	}
	wg := new(sync.WaitGroup)
	bs.workers = &workers{
		splitCheckWorker:  newWorker("split-check", wg),
		regionWorker:      newWorker("snapshot-worker", wg),
		raftLogGCWorker:   newWorker("raft-gc-worker", wg),
		pdWorker:          pdWorker,
		computeHashWorker: newWorker("compute-hash", wg),
		wg:                wg,
	}
	bs.ctx = &GlobalContext{
		cfg:                   cfg,
		engine:                engines,
		store:                 meta,
		storeMeta:             newStoreMeta(),
		storeMetaLock:         new(sync.RWMutex),
		router:                bs.router,
		trans:                 trans,
		pdTaskSender:          bs.workers.pdWorker.sender,
		regionTaskSender:      bs.workers.regionWorker.sender,
		computeHashTaskSender: bs.workers.computeHashWorker.sender,
		splitCheckTaskSender:  bs.workers.splitCheckWorker.sender,
		raftLogGCTaskSender:   bs.workers.raftLogGCWorker.sender,
		pdClient:              pdClient,
		peerEventObserver:     observer,
		globalStats:           new(storeStats),
	}
	regionPeers, err := bs.loadPeers()
	if err != nil {
		return err
	}
	log.S().Infof("region peers %d", len(regionPeers))

	for _, peer := range regionPeers {
		bs.router.register(peer)
	}
	bs.startWorkers(regionPeers)
	return nil
}

func (bs *raftBatchSystem) startWorkers(peers []*peerFsm) {
	ctx := bs.ctx
	workers := bs.workers
	router := bs.router

	bs.wg.Add(3) // raftWorker, applyWorker, storeWorker
	rw := newRaftWorker(ctx, router.peerSender, router)
	go rw.run(bs.closeCh, bs.wg)
	aw := newApplyWorker(router, rw.applyCh, rw.applyCtx)
	go aw.run(bs.wg)
	sw := newStoreWorker(ctx, router)
	go sw.run(bs.closeCh, bs.wg)

	router.sendStore(Msg{Type: MsgTypeStoreStart, Data: ctx.store})
	for i := 0; i < len(peers); i++ {
		regionID := peers[i].peer.regionId
		_ = router.send(regionID, Msg{RegionID: regionID, Type: MsgTypeStart})
	}
	engines := ctx.engine
	cfg := ctx.cfg
	workers.splitCheckWorker.start(newSplitCheckRunner(engines.kv, router, cfg.SplitCheck))
	workers.regionWorker.start(newRegionTaskHandler(bs.globalCfg, engines, router))
	workers.raftLogGCWorker.start(&raftLogGCTaskHandler{})
	workers.pdWorker.start(newPDTaskHandler(ctx.store.Id, ctx.pdClient, bs.router))
	workers.computeHashWorker.start(&computeHashTaskHandler{router: bs.router})
}

func (bs *raftBatchSystem) shutDown() {
	if bs.workers == nil {
		return
	}
	close(bs.closeCh)
	bs.wg.Wait()
	workers := bs.workers
	bs.workers = nil
	stopTask := task{tp: taskTypeStop}
	workers.splitCheckWorker.sender <- stopTask
	workers.regionWorker.sender <- stopTask
	workers.raftLogGCWorker.sender <- stopTask
	workers.computeHashWorker.sender <- stopTask
	workers.pdWorker.sender <- stopTask
	workers.wg.Wait()
}

func createRaftBatchSystem(globalCfg *config.Config, raftCfg *Config) (*router, *raftBatchSystem) {
	storeSender, storeFsm := newStoreFsm(raftCfg)
	router := newRouter(storeSender, storeFsm)
	raftBatchSystem := &raftBatchSystem{
		router:    router,
		closeCh:   make(chan struct{}),
		wg:        new(sync.WaitGroup),
		globalCfg: globalCfg,
	}
	return router, raftBatchSystem
}

/// Checks if the message is targeting a stale peer.
///
/// Returns true means the message can be dropped silently.
func (d *storeMsgHandler) checkMsg(msg *rspb.RaftMessage) (bool, error) {
	regionID := msg.GetRegionId()
	fromEpoch := msg.GetRegionEpoch()
	msgType := msg.Message.MsgType
	isVoteMsg := isVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.StoreId

	// Check if the target is tombstone,
	stateKey := RegionStateKeyByIDEpoch(regionID, fromEpoch)
	localState := new(rspb.RegionLocalState)
	err := getMsg(d.ctx.engine.raft, stateKey, localState)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	if localState.State != rspb.PeerState_Tombstone {
		// Maybe split, but not registered yet.
		if isFirstVoteMessage(msg.Message) {
			d.ctx.storeMetaLock.Lock()
			defer d.ctx.storeMetaLock.Unlock()
			meta := d.ctx.storeMeta
			// Last check on whether target peer is created, otherwise, the
			// vote message will never be comsumed.
			if _, ok := meta.regions[regionID]; ok {
				return false, nil
			}
			meta.pendingVotes = append(meta.pendingVotes, msg)
			log.S().Infof("region %d doesn't exist yet, wait for it to be split.", regionID)
			return true, nil
		}
		return false, errors.Errorf("region %d not exists but not tombstone: %s", regionID, localState)
	}
	log.S().Debugf("region %d in tombstone state: %s", regionID, localState)
	region := localState.Region
	regionEpoch := region.RegionEpoch
	if localState.MergeState != nil {
		// TODO: Merge
		return true, nil
	}
	// The region in this peer is already destroyed
	if IsEpochStale(fromEpoch, regionEpoch) {
		log.S().Infof("tombstone peer receives a stale message. region_id:%d, from_region_epoch:%s, current_region_epoch:%s, msg_type:%s",
			regionID, fromEpoch, regionEpoch, msgType)
		notExist := findPeer(region, fromStoreID) == nil
		handleStaleMsg(d.ctx.trans, msg, regionEpoch, isVoteMsg && notExist, nil)
		return true, nil
	}
	if fromEpoch.ConfVer == regionEpoch.ConfVer {
		return false, errors.Errorf("tombstone peer [epoch: %s] received an invalid message %s, ignore it",
			regionEpoch, msgType)
	}
	return false, nil
}

func (d *storeMsgHandler) onRaftMessage(msg *rspb.RaftMessage) error {
	regionID := msg.RegionId
	if err := d.ctx.router.send(regionID, Msg{Type: MsgTypeRaftMessage, Data: msg}); err == nil {
		return nil
	}
	log.S().Debugf("handle raft message. from_peer:%d, to_peer:%d, store:%d, region:%d, msg_type:%s",
		msg.FromPeer.Id, msg.ToPeer.Id, d.storeFsm.id, regionID, msg.Message.MsgType)
	if msg.ToPeer.StoreId != d.ctx.store.Id {
		log.S().Warnf("store not match, ignore it. store_id:%d, to_store_id:%d, region_id:%d",
			d.ctx.store.Id, msg.ToPeer.StoreId, regionID)
		return nil
	}

	if msg.RegionEpoch == nil {
		log.S().Errorf("missing region epoch in raft message, ignore it. region_id:%d", regionID)
		return nil
	}
	if msg.IsTombstone || msg.MergeTarget != nil {
		// Target tombstone peer doesn't exist, so ignore it.
		return nil
	}
	ok, err := d.checkMsg(msg)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	created, err := d.maybeCreatePeer(regionID, msg)
	if err != nil {
		return err
	}
	if !created {
		return nil
	}
	_ = d.ctx.router.send(regionID, Msg{Type: MsgTypeRaftMessage, Data: msg})
	return nil
}

/// If target peer doesn't exist, create it.
///
/// return false to indicate that target peer is in invalid state or
/// doesn't exist and can't be created.
func (d *storeMsgHandler) maybeCreatePeer(regionID uint64, msg *rspb.RaftMessage) (bool, error) {
	var regionsToDestroy []uint64
	// we may encounter a message with larger peer id, which means
	// current peer is stale, then we should remove current peer
	d.ctx.storeMetaLock.Lock()
	defer func() {
		d.ctx.storeMetaLock.Unlock()
		// send message out of store meta lock, to avoid dead lock.
		destroyRegions(d.ctx.router, regionsToDestroy, msg.ToPeer)
	}()
	meta := d.ctx.storeMeta
	if _, ok := meta.regions[regionID]; ok {
		return true, nil
	}
	if !isInitialMsg(msg.Message) {
		log.S().Debugf("target peer %s doesn't exist", msg.ToPeer)
		return false, nil
	}
	isReturned := false
	meta.regionTree.Iterate(msg.StartKey, msg.EndKey, func(existRegion *metapb.Region) bool {
		log.S().Debugf("msg %s is overlapped with exist region %s", msg, existRegion)
		if isFirstVoteMessage(msg.Message) {
			meta.pendingVotes = append(meta.pendingVotes, msg)
		}

		// Make sure the range of region from msg is covered by existing regions.
		// If so, means that the region may be generated by some kinds of split
		// and merge by catching logs. So there is no need to accept a snapshot.
		if !isRangeCovered(meta, msg.StartKey, msg.EndKey) {
			if maybeDestroySource(meta, regionID, existRegion.Id, msg.RegionEpoch) {
				regionsToDestroy = append(regionsToDestroy, existRegion.Id)
				return true
			}
		}
		regionsToDestroy = nil
		isReturned = true
		return false
	})
	if isReturned {
		return false, nil
	}

	// New created peers should know it's learner or not.
	peer, err := replicatePeerFsm(
		d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, regionID, msg.StartKey, msg.EndKey, msg.ToPeer)
	if err != nil {
		return false, err
	}
	// following snapshot may overlap, should insert into region_ranges after
	// snapshot is applied.
	meta.regions[regionID] = peer.peer.Region()
	d.ctx.router.register(peer)
	_ = d.ctx.router.send(regionID, Msg{Type: MsgTypeStart})
	d.ctx.peerEventObserver.OnPeerCreate(peer.peer.getEventContext(), peer.peer.Region())
	return true, nil
}

func destroyRegions(router *router, regionsToDestroy []uint64, toPeer *metapb.Peer) {
	for _, id := range regionsToDestroy {
		_ = router.send(id, Msg{Type: MsgTypeMergeResult, Data: &MsgMergeResult{
			TargetPeer: toPeer,
			Stale:      true,
		}})
	}
}

func (d *storeMsgHandler) storeHeartbeatPD() {
	stats := new(pdpb.StoreStats)
	// TODO: cache used size
	stats.UsedSize = uint64(d.ctx.engine.kv.Size())
	stats.StoreId = d.ctx.store.Id
	d.ctx.storeMetaLock.RLock()
	stats.RegionCount = uint32(len(d.ctx.storeMeta.regions))
	d.ctx.storeMetaLock.RUnlock()
	// TODO: update snap stats
	stats.SendingSnapCount = 0
	stats.ReceivingSnapCount = 0
	stats.ApplyingSnapCount = uint32(atomic.LoadUint64(d.ctx.applyingSnapCount))
	stats.StartTime = uint32(d.startTime.Second())
	globalStats := d.ctx.globalStats
	stats.BytesWritten = atomic.SwapUint64(&globalStats.engineTotalBytesWritten, 0)
	stats.KeysWritten = atomic.SwapUint64(&globalStats.engineTotalKeysWritten, 0)
	stats.IsBusy = atomic.SwapUint64(&globalStats.isBusy, 0) > 0
	storeInfo := &pdStoreHeartbeatTask{
		stats:    stats,
		engine:   d.ctx.engine.kv,
		capacity: d.ctx.cfg.Capacity,
		path:     d.ctx.engine.kvPath,
	}
	d.ctx.pdTaskSender <- task{tp: taskTypePDStoreHeartbeat, data: storeInfo}
}

func (d *storeMsgHandler) onPDStoreHearbeatTick() {
	d.storeHeartbeatPD()
	d.ticker.scheduleStore(StoreTickPdStoreHeartbeat)
}

func (d *storeMsgHandler) onComputeHashTick() {
	d.ticker.scheduleStore(StoreTickConsistencyCheck)
	if len(d.ctx.computeHashTaskSender) > 0 {
		return
	}
	targetRegion := d.findTargetRegionForComputeHash()
	if targetRegion == nil {
		return
	}
	peer := findPeer(targetRegion, d.ctx.store.Id)
	if peer == nil {
		return
	}
	log.S().Infof("schedule consistency check for region %d, store %d", targetRegion.Id, peer.StoreId)
	d.storeFsm.consistencyCheckTime[targetRegion.Id] = time.Now()
	request := newAdminRequest(targetRegion.Id, peer)
	request.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_ComputeHash,
	}
	cmd := &MsgRaftCmd{
		Request: raftlog.NewRequest(request),
	}
	_ = d.ctx.router.sendRaftCommand(cmd)
}

func (d *storeMsgHandler) findTargetRegionForComputeHash() *metapb.Region {
	oldest := time.Now()
	var targetRegion *metapb.Region
	d.ctx.storeMetaLock.RLock()
	defer d.ctx.storeMetaLock.RUnlock()
	meta := d.ctx.storeMeta
	for regionID, region := range meta.regions {
		if t, ok := d.storeFsm.consistencyCheckTime[regionID]; ok {
			if t.Before(oldest) {
				oldest = t
				targetRegion = region
			}
		} else {
			targetRegion = region
			break
		}
	}
	return targetRegion
}

func (d *storeMsgHandler) findRegionsInRange(startKey, endKey []byte) []*metapb.Region {
	d.ctx.storeMetaLock.RLock()
	defer d.ctx.storeMetaLock.RUnlock()
	meta := d.ctx.storeMeta
	var regions []*metapb.Region
	meta.regionTree.Iterate(startKey, endKey, func(region *metapb.Region) bool {
		regions = append(regions, region)
		return true
	})
	return regions
}

func isRangeCovered(meta *storeMeta, start, end []byte) bool {
	var covered bool
	meta.regionTree.Iterate(start, end, func(region *metapb.Region) bool {
		// find a missing range
		if bytes.Compare(start, region.StartKey) < 0 {
			return false
		}
		if bytes.Compare(region.EndKey, end) >= 0 {
			covered = true
			return false
		}
		start = region.EndKey
		return true
	})
	return covered
}

func (d *storeMsgHandler) onGenerateEngineMetaChange(msg Msg) {
	// GenerateEngineMetaChange message is first sent to store handler to find a region id for this change,
	// Once we got the region ID, we send it to the router to create a raft log then propose this log, replicate to
	// followers.
	e := msg.Data.(*sdbpb.ChangeSet)
	err := d.ctx.router.send(e.ShardID, msg)
	if err != nil {
		log.S().Errorf("failed to send change event for %d:%d err %v", e.ShardID, e.ShardVer, err)
	}
}
