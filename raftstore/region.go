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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ngaut/unistore/tikv/raftstore"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/mockstore/unistore/metrics"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv"
	"github.com/pingcap/tidb/util/codec"
	"github.com/zhangjinpeng1987/raft"
	"go.uber.org/zap"
)

// InternalKey
var (
	InternalKeyPrefix        = []byte{0xff}
	InternalRegionMetaPrefix = append(InternalKeyPrefix, "region"...)
	InternalStoreMetaKey     = append(InternalKeyPrefix, "store"...)
	InternalSafePointKey     = append(InternalKeyPrefix, "safepoint"...)
)

type regionCtx struct {
	meta        *metapb.Region
	regionEpoch unsafe.Pointer // *metapb.RegionEpoch
	rawStartKey []byte
	rawEndKey   []byte
	diff        int64

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
	regCtx.rawStartKey = regCtx.decodeRawStartKey()
	regCtx.rawEndKey = regCtx.decodeRawEndKey()
	if len(regCtx.rawEndKey) == 0 {
		// Avoid reading internal meta data.
		regCtx.rawEndKey = InternalKeyPrefix
	}
	return regCtx
}

func (ri *regionCtx) Meta() *metapb.Region {
	return ri.meta
}

func (ri *regionCtx) Diff() *int64 {
	return &ri.diff
}

func (ri *regionCtx) RawStart() []byte {
	return ri.rawStartKey
}

func (ri *regionCtx) RawEnd() []byte {
	return ri.rawEndKey
}

func (ri *regionCtx) getRegionEpoch() *metapb.RegionEpoch {
	return (*metapb.RegionEpoch)(atomic.LoadPointer(&ri.regionEpoch))
}

func (ri *regionCtx) updateRegionEpoch(epoch *metapb.RegionEpoch) {
	atomic.StorePointer(&ri.regionEpoch, (unsafe.Pointer)(epoch))
}

func (ri *regionCtx) decodeRawStartKey() []byte {
	if len(ri.meta.StartKey) == 0 {
		return nil
	}
	_, rawKey, err := codec.DecodeBytes(ri.meta.StartKey, nil)
	if err != nil {
		panic("invalid region start key")
	}
	return rawKey
}

func (ri *regionCtx) decodeRawEndKey() []byte {
	if len(ri.meta.EndKey) == 0 {
		return nil
	}
	_, rawKey, err := codec.DecodeBytes(ri.meta.EndKey, nil)
	if err != nil {
		panic("invalid region end key")
	}
	return rawKey
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

// RegionOptions represents the region options.
type RegionOptions struct {
	StoreAddr  string
	PDAddr     string
	RegionSize int64
}

type regionManager struct {
	storeMeta *metapb.Store
	mu        sync.RWMutex
	regions   map[uint64]*regionCtx
	latches   *latches
}

func (rm *regionManager) GetStoreIDByAddr(addr string) (uint64, error) {
	if rm.storeMeta.Address != addr {
		return 0, errors.New("store not match")
	}
	return rm.storeMeta.Id, nil
}

func (rm *regionManager) GetStoreAddrByStoreID(storeID uint64) (string, error) {
	if rm.storeMeta.Id != storeID {
		return "", errors.New("store not match")
	}
	return rm.storeMeta.Address, nil
}

func (rm *regionManager) GetStoreInfoFromCtx(ctx *kvrpcpb.Context) (string, uint64, *errorpb.Error) {
	if ctx.GetPeer() != nil && ctx.GetPeer().GetStoreId() != rm.storeMeta.Id {
		return "", 0, &errorpb.Error{
			Message:       "store not match",
			StoreNotMatch: &errorpb.StoreNotMatch{},
		}
	}
	return rm.storeMeta.Address, rm.storeMeta.Id, nil
}

func (rm *regionManager) GetRegionFromCtx(ctx *kvrpcpb.Context) (tikv.RegionCtx, *errorpb.Error) {
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

// RaftRegionManager represents a raft region manager.
type RaftRegionManager struct {
	regionManager
	router   *raftstore.Router
	eventCh  chan interface{}
	detector *tikv.DetectorServer
}

// NewRaftRegionManager returns a new raft region manager.
func NewRaftRegionManager(store *metapb.Store, router *raftstore.Router, detector *tikv.DetectorServer) *RaftRegionManager {
	m := &RaftRegionManager{
		router: router,
		regionManager: regionManager{
			storeMeta: store,
			regions:   make(map[uint64]*regionCtx),
			latches:   newLatches(),
		},
		eventCh:  make(chan interface{}, 1024),
		detector: detector,
	}
	go m.runEventHandler()
	return m
}

type peerCreateEvent struct {
	ctx    *raftstore.PeerEventContext
	region *metapb.Region
}

// OnPeerCreate will be invoked when there is a new peer created.
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

// OnPeerApplySnap will be invoked when there is a replicate peer's snapshot applied.
func (rm *RaftRegionManager) OnPeerApplySnap(ctx *raftstore.PeerEventContext, region *metapb.Region) {
	rm.eventCh <- &peerApplySnapEvent{
		ctx:    ctx,
		region: region,
	}
}

type peerDestroyEvent struct {
	regionID uint64
}

// OnPeerDestroy will be invoked when a peer is destroyed.
func (rm *RaftRegionManager) OnPeerDestroy(ctx *raftstore.PeerEventContext) {
	rm.eventCh <- &peerDestroyEvent{regionID: ctx.RegionID}
}

type splitRegionEvent struct {
	derived *metapb.Region
	regions []*metapb.Region
	peers   []*raftstore.PeerEventContext
}

// OnSplitRegion will be invoked when region split into new regions with corresponding peers.
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

// OnRegionConfChange will be invoked after conf change updated region's epoch.
func (rm *RaftRegionManager) OnRegionConfChange(ctx *raftstore.PeerEventContext, epoch *metapb.RegionEpoch) {
	rm.eventCh <- &regionConfChangeEvent{
		ctx:   ctx,
		epoch: epoch,
	}
}

type regionRoleChangeEvent struct {
	regionID uint64
	newState raft.StateType
}

// OnRoleChange will be invoked after peer state has changed
func (rm *RaftRegionManager) OnRoleChange(regionID uint64, newState raft.StateType) {
	rm.eventCh <- &regionRoleChangeEvent{regionID: regionID, newState: newState}
}

// GetRegionFromCtx implements the RegionManager interface.
func (rm *RaftRegionManager) GetRegionFromCtx(ctx *kvrpcpb.Context) (tikv.RegionCtx, *errorpb.Error) {
	ri, err := rm.regionManager.GetRegionFromCtx(ctx)
	if err != nil {
		return nil, err
	}
	if err := ri.(*regionCtx).leaderChecker.IsLeader(ctx, rm.router); err != nil {
		return nil, err
	}
	return ri, nil
}

// Close implements the RegionManager interface.
func (rm *RaftRegionManager) Close() error {
	return nil
}

func (rm *RaftRegionManager) runEventHandler() {
	for event := range rm.eventCh {
		switch x := event.(type) {
		case *peerCreateEvent:
			regCtx := newRegionCtx(x.region, rm.latches, x.ctx.LeaderChecker)
			rm.mu.Lock()
			rm.regions[x.ctx.RegionID] = regCtx
			rm.mu.Unlock()
		case *splitRegionEvent:
			rm.mu.Lock()
			for i, region := range x.regions {
				rm.regions[region.Id] = newRegionCtx(region, rm.latches, x.peers[i].LeaderChecker)
			}
			rm.mu.Unlock()
		case *regionConfChangeEvent:
			rm.mu.RLock()
			region := rm.regions[x.ctx.RegionID]
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
			region := rm.regions[x.regionID]
			rm.mu.RUnlock()
			if bytes.Equal(region.rawStartKey, []byte{}) && len(region.meta.Peers) > 0 {
				newRole := tikv.Follower
				if x.newState == raft.StateLeader {
					newRole = tikv.Leader
				}
				log.Info("first region role changed", zap.Int("new role", newRole))
				rm.detector.ChangeRole(int32(newRole))
			}
		}
	}
}

// SplitRegion implements the RegionManager interface.
func (rm *RaftRegionManager) SplitRegion(req *kvrpcpb.SplitRegionRequest) *kvrpcpb.SplitRegionResponse {
	splitKeys := make([][]byte, 0, len(req.SplitKeys))
	for _, rawKey := range req.SplitKeys {
		splitKeys = append(splitKeys, codec.EncodeBytes(nil, rawKey))
	}
	regions, err := rm.router.SplitRegion(req.GetContext(), splitKeys)
	if err != nil {
		return &kvrpcpb.SplitRegionResponse{RegionError: &errorpb.Error{Message: err.Error()}}
	}
	return &kvrpcpb.SplitRegionResponse{Regions: regions}
}
