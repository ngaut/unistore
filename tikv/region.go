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

package tikv

import (
	"bytes"
	"encoding/binary"
	"github.com/ngaut/unistore/sdb"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/zhangjinpeng1987/raft"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ngaut/unistore/metrics"
	"github.com/ngaut/unistore/tikv/raftstore"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
)

var (
	InternalKeyPrefix        = []byte{0xff}
	InternalRegionMetaPrefix = append(InternalKeyPrefix, "region"...)
	InternalStoreMetaKey     = append(InternalKeyPrefix, "store"...)
)

func InternalRegionMetaKey(regionId uint64) []byte {
	return []byte(string(InternalRegionMetaPrefix) + strconv.FormatUint(regionId, 10))
}

type regionCtx struct {
	meta            *metapb.Region
	regionEpoch     unsafe.Pointer // *metapb.RegionEpoch
	startKey        []byte
	endKey          []byte
	approximateSize int64
	diff            int64

	latches       *latches
	leaderChecker raftstore.LeaderChecker
}

type latches struct {
	slots [256]map[uint64]*sync.WaitGroup
	locks [256]sync.Mutex
}

func newLatches() *latches {
	l := &latches{}
	for i := 0; i < 256; i++ {
		l.slots[i] = map[uint64]*sync.WaitGroup{}
	}
	return l
}

func (l *latches) acquire(keyHashes []uint64) (waitCnt int) {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	for _, hash := range keyHashes {
		waitCnt += l.acquireOne(hash, wg)
	}
	return
}

func (l *latches) acquireOne(hash uint64, wg *sync.WaitGroup) (waitCnt int) {
	slotID := hash >> 56
	for {
		m := l.slots[slotID]
		l.locks[slotID].Lock()
		w, ok := m[hash]
		if !ok {
			m[hash] = wg
		}
		l.locks[slotID].Unlock()
		if ok {
			w.Wait()
			waitCnt++
			continue
		}
		return
	}
}

func (l *latches) release(keyHashes []uint64) {
	var w *sync.WaitGroup
	for _, hash := range keyHashes {
		slotID := hash >> 56
		l.locks[slotID].Lock()
		m := l.slots[slotID]
		if w == nil {
			w = m[hash]
		}
		delete(m, hash)
		l.locks[slotID].Unlock()
	}
	if w != nil {
		w.Done()
	}
}

func newRegionCtx(meta *metapb.Region, latches *latches, checker raftstore.LeaderChecker) *regionCtx {
	regCtx := &regionCtx{
		meta:          meta,
		latches:       latches,
		regionEpoch:   unsafe.Pointer(meta.GetRegionEpoch()),
		leaderChecker: checker,
	}
	regCtx.startKey = regCtx.rawStartKey()
	regCtx.endKey = regCtx.rawEndKey()
	if len(regCtx.endKey) == 0 {
		// Avoid reading internal meta data.
		regCtx.endKey = InternalKeyPrefix
	}
	return regCtx
}

func (ri *regionCtx) getRegionEpoch() *metapb.RegionEpoch {
	return (*metapb.RegionEpoch)(atomic.LoadPointer(&ri.regionEpoch))
}

func (ri *regionCtx) updateRegionEpoch(epoch *metapb.RegionEpoch) {
	atomic.StorePointer(&ri.regionEpoch, (unsafe.Pointer)(epoch))
}

func (ri *regionCtx) rawStartKey() []byte {
	if len(ri.meta.StartKey) == 0 {
		return nil
	}
	_, rawKey, err := codec.DecodeBytes(ri.meta.StartKey, nil)
	if err != nil {
		panic("invalid region start key")
	}
	return rawKey
}

func (ri *regionCtx) rawEndKey() []byte {
	if len(ri.meta.EndKey) == 0 {
		return nil
	}
	_, rawKey, err := codec.DecodeBytes(ri.meta.EndKey, nil)
	if err != nil {
		panic("invalid region end key")
	}
	return rawKey
}

func (ri *regionCtx) lessThanStartKey(key []byte) bool {
	return bytes.Compare(key, ri.startKey) < 0
}

func (ri *regionCtx) greaterEqualEndKey(key []byte) bool {
	return len(ri.endKey) > 0 && bytes.Compare(key, ri.endKey) >= 0
}

func (ri *regionCtx) greaterThanEndKey(key []byte) bool {
	return len(ri.endKey) > 0 && bytes.Compare(key, ri.endKey) > 0
}

func (ri *regionCtx) unmarshal(data []byte) error {
	ri.approximateSize = int64(binary.LittleEndian.Uint64(data))
	data = data[8:]
	ri.meta = &metapb.Region{}
	err := ri.meta.Unmarshal(data)
	if err != nil {
		return errors.Trace(err)
	}
	ri.startKey = ri.rawStartKey()
	ri.endKey = ri.rawEndKey()
	ri.regionEpoch = unsafe.Pointer(ri.meta.RegionEpoch)
	return nil
}

func (ri *regionCtx) marshal() []byte {
	data := make([]byte, 8+ri.meta.Size())
	binary.LittleEndian.PutUint64(data, uint64(ri.approximateSize))
	_, err := ri.meta.MarshalTo(data[8:])
	if err != nil {
		log.Error("region ctx marshal failed", zap.Error(err))
	}
	return data
}

// AcquireLatches add latches for all input hashVals, the input hashVals should be
// sorted and have no duplicates
func (ri *regionCtx) AcquireLatches(hashVals []uint64) {
	start := time.Now()
	waitCnt := ri.latches.acquire(hashVals)
	dur := time.Since(start)
	metrics.LatchWait.Observe(dur.Seconds())
	if dur > time.Millisecond*50 {
		log.S().Warnf("region %d acquire %d locks takes %v, waitCnt %d", ri.meta.Id, len(hashVals), dur, waitCnt)
	}
}

func (ri *regionCtx) ReleaseLatches(hashVals []uint64) {
	ri.latches.release(hashVals)
}

type RegionOptions struct {
	StoreAddr  string
	PDAddr     string
	RegionSize int64
}

type RegionManager interface {
	GetRegionFromCtx(ctx *kvrpcpb.Context) (*regionCtx, *errorpb.Error)
	SplitRegion(req *kvrpcpb.SplitRegionRequest, ctx *requestCtx) *kvrpcpb.SplitRegionResponse
	Close() error
}

type regionManager struct {
	storeMeta *metapb.Store
	mu        sync.RWMutex
	regions   map[uint64]*regionCtx
	latches   *latches
}

func (rm *regionManager) GetRegionFromCtx(ctx *kvrpcpb.Context) (*regionCtx, *errorpb.Error) {
	ctxPeer := ctx.GetPeer()
	if ctxPeer != nil && ctxPeer.GetStoreId() != rm.storeMeta.Id {
		return nil, &errorpb.Error{
			Message:       "store not match",
			StoreNotMatch: &errorpb.StoreNotMatch{},
		}
	}
	rm.mu.RLock()
	ri := rm.regions[ctx.RegionId]
	rm.mu.RUnlock()
	if ri == nil {
		return nil, &errorpb.Error{
			Message: "region not found",
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: ctx.GetRegionId(),
			},
		}
	}
	// Region epoch does not match.
	if rm.isEpochStale(ri.getRegionEpoch(), ctx.GetRegionEpoch()) {
		return nil, &errorpb.Error{
			Message: "stale epoch",
			EpochNotMatch: &errorpb.EpochNotMatch{
				CurrentRegions: []*metapb.Region{{
					Id:          ri.meta.Id,
					StartKey:    ri.meta.StartKey,
					EndKey:      ri.meta.EndKey,
					RegionEpoch: ri.getRegionEpoch(),
					Peers:       ri.meta.Peers,
				}},
			},
		}
	}
	return ri, nil
}

func (rm *regionManager) isEpochStale(lhs, rhs *metapb.RegionEpoch) bool {
	return lhs.GetConfVer() != rhs.GetConfVer() || lhs.GetVersion() != rhs.GetVersion()
}

func (rm *regionManager) loadFromLocal(db *sdb.DB, f func(*regionCtx)) error {
	snap := db.NewSnapshot(db.GetShard(1))
	defer snap.Discard()
	item, err := snap.Get(mvcc.RaftCF, y.KeyWithTs(InternalStoreMetaKey, 0))
	if err != nil && err != badger.ErrKeyNotFound {
		return err
	}
	if err != nil {
		return nil
	}
	val, _ := item.Value()
	err = rm.storeMeta.Unmarshal(val)
	if err != nil {
		return err
	}
	// load region meta
	it := snap.NewIterator(mvcc.RaftCF, false, false)
	defer it.Close()
	prefix := InternalRegionMetaPrefix
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		val, err1 := item.Value()
		if err1 != nil {
			return err1
		}
		r := new(regionCtx)
		err := r.unmarshal(val)
		if err != nil {
			return errors.Trace(err)
		}
		r.latches = rm.latches
		rm.regions[r.meta.Id] = r
		f(r)
	}
	return nil
}

type RaftRegionManager struct {
	regionManager
	router   *raftstore.RaftstoreRouter
	eventCh  chan interface{}
	detector *DetectorServer
	splitMu  sync.Mutex
	splits   map[uint64]bool
}

func NewRaftRegionManager(store *metapb.Store, router *raftstore.RaftstoreRouter, detector *DetectorServer) *RaftRegionManager {
	m := &RaftRegionManager{
		router: router,
		regionManager: regionManager{
			storeMeta: store,
			regions:   make(map[uint64]*regionCtx),
			latches:   newLatches(),
		},
		eventCh:  make(chan interface{}, 1024),
		detector: detector,
		splits:   map[uint64]bool{},
	}
	go m.runEventHandler()
	return m
}

type peerCreateEvent struct {
	ctx    *raftstore.PeerEventContext
	region *metapb.Region
}

func (rm *RaftRegionManager) OnPeerCreate(ctx *raftstore.PeerEventContext, region *metapb.Region) {
	rm.eventCh <- &peerCreateEvent{
		ctx:    ctx,
		region: region,
	}
}

type peerApplySnapEvent struct {
	ctx    *raftstore.PeerEventContext
	region *metapb.Region
}

func (rm *RaftRegionManager) OnPeerApplySnap(ctx *raftstore.PeerEventContext, region *metapb.Region) {
	rm.eventCh <- &peerApplySnapEvent{
		ctx:    ctx,
		region: region,
	}
}

type peerDestroyEvent struct {
	regionID uint64
}

func (rm *RaftRegionManager) OnPeerDestroy(ctx *raftstore.PeerEventContext) {
	rm.eventCh <- &peerDestroyEvent{regionID: ctx.RegionId}
}

type splitRegionEvent struct {
	derived *metapb.Region
	regions []*metapb.Region
	peers   []*raftstore.PeerEventContext
}

func (rm *RaftRegionManager) OnSplitRegion(derived *metapb.Region, regions []*metapb.Region, peers []*raftstore.PeerEventContext) {
	rm.eventCh <- &splitRegionEvent{
		derived: derived,
		regions: regions,
		peers:   peers,
	}
}

type regionConfChangeEvent struct {
	ctx   *raftstore.PeerEventContext
	epoch *metapb.RegionEpoch
}

func (rm *RaftRegionManager) OnRegionConfChange(ctx *raftstore.PeerEventContext, epoch *metapb.RegionEpoch) {
	rm.eventCh <- &regionConfChangeEvent{
		ctx:   ctx,
		epoch: epoch,
	}
}

type regionRoleChangeEvent struct {
	regionId uint64
	newState raft.StateType
}

func (rm *RaftRegionManager) OnRoleChange(regionId uint64, newState raft.StateType) {
	rm.eventCh <- &regionRoleChangeEvent{regionId: regionId, newState: newState}
}

func (rm *RaftRegionManager) GetRegionFromCtx(ctx *kvrpcpb.Context) (*regionCtx, *errorpb.Error) {
	regionCtx, err := rm.regionManager.GetRegionFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	if err := regionCtx.leaderChecker.IsLeader(ctx, rm.router); err != nil {
		return nil, err
	}
	return regionCtx, nil
}

func (rm *RaftRegionManager) Close() error {
	return nil
}

func (rm *RaftRegionManager) runEventHandler() {
	for event := range rm.eventCh {
		switch x := event.(type) {
		case *peerCreateEvent:
			regCtx := newRegionCtx(x.region, rm.latches, x.ctx.LeaderChecker)
			rm.mu.Lock()
			rm.regions[x.ctx.RegionId] = regCtx
			rm.mu.Unlock()
		case *splitRegionEvent:
			rm.mu.Lock()
			for i, region := range x.regions {
				rm.regions[region.Id] = newRegionCtx(region, rm.latches, x.peers[i].LeaderChecker)
			}
			rm.mu.Unlock()
		case *regionConfChangeEvent:
			rm.mu.RLock()
			region := rm.regions[x.ctx.RegionId]
			rm.mu.RUnlock()
			region.updateRegionEpoch(x.epoch)
		case *peerDestroyEvent:
			rm.mu.Lock()
			delete(rm.regions, x.regionID)
			rm.mu.Unlock()
		case *peerApplySnapEvent:
			rm.mu.Lock()
			rm.regions[x.region.Id] = newRegionCtx(x.region, rm.latches, x.ctx.LeaderChecker)
			rm.mu.Unlock()
		case *regionRoleChangeEvent:
			rm.mu.RLock()
			region := rm.regions[x.regionId]
			rm.mu.RUnlock()
			if bytes.Compare(region.startKey, []byte{}) == 0 && len(region.meta.Peers) > 0 {
				newRole := Follower
				if x.newState == raft.StateLeader {
					newRole = Leader
				}
				log.Info("first region role changed", zap.Int("new role", newRole))
				rm.detector.ChangeRole(int32(newRole))
			}
		}
	}
}

func (rm *RaftRegionManager) SplitRegion(req *kvrpcpb.SplitRegionRequest, ctx *requestCtx) *kvrpcpb.SplitRegionResponse {
	rm.splitMu.Lock()
	splitting := rm.splits[req.Context.RegionId]
	if !splitting {
		rm.splits[req.Context.RegionId] = true
	}
	rm.splitMu.Unlock()
	if splitting {
		return &kvrpcpb.SplitRegionResponse{RegionError: &errorpb.Error{Message: "splitting"}}
	}
	regions, err := rm.router.SplitRegion(req.GetContext(), ctx.svr.mvccStore.db, ctx.regCtx.meta, req.SplitKeys)
	rm.splitMu.Lock()
	delete(rm.splits, req.Context.RegionId)
	rm.splitMu.Unlock()
	if err != nil {
		return &kvrpcpb.SplitRegionResponse{RegionError: &errorpb.Error{Message: err.Error()}}
	}
	return &kvrpcpb.SplitRegionResponse{Regions: regions}
}
