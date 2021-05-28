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
	"fmt"
	"github.com/ngaut/unistore/sdb"
	"github.com/ngaut/unistore/sdbpb"
	"math"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/log"
	"github.com/zhangjinpeng1987/raft"
)

type JobStatus = uint32

const (
	JobStatus_Pending JobStatus = 0 + iota
	JobStatus_Running
	JobStatus_Cancelling
	JobStatus_Cancelled
	JobStatus_Finished
	JobStatus_Failed
)

type SnapStateType int

const (
	SnapState_Relax SnapStateType = 0 + iota
	SnapState_Generating
	SnapState_Applying
	SnapState_ApplyAborted
)

type SnapState struct {
	StateType SnapStateType
	Status    *JobStatus
	Receiver  chan *eraftpb.Snapshot
}

const (
	// When we create a region peer, we should initialize its log term/index > 0,
	// so that we can force the follower peer to sync the snapshot first.
	RaftInitLogTerm  = 5
	RaftInitLogIndex = 5

	MaxSnapRetryCnt = 5

	raftLogMultiGetCnt = 8

	MaxCacheCapacity = 1024 - 1
)

// CompactRaftLog discards all log entries prior to compact_index. We must guarantee
// that the compact_index is not greater than applied index.
func CompactRaftLog(tag string, state *applyState, compactIndex, compactTerm uint64) error {
	log.S().Debugf("%s compact log entries to prior to %d", tag, compactIndex)

	if compactIndex <= state.truncatedIndex {
		return errors.New("try to truncate compacted entries")
	} else if compactIndex > state.appliedIndex {
		return errors.Errorf("compact index %d > applied index %d", compactIndex, state.appliedIndex)
	}

	// we don't actually delete the logs now, we add an async task to do it.
	state.truncatedIndex = compactIndex
	state.truncatedTerm = compactTerm
	return nil
}

type EntryCache struct {
	cache []eraftpb.Entry
}

func (ec *EntryCache) front() eraftpb.Entry {
	return ec.cache[0]
}

func (ec *EntryCache) back() eraftpb.Entry {
	return ec.cache[len(ec.cache)-1]
}

func (ec *EntryCache) length() int {
	return len(ec.cache)
}

func (ec *EntryCache) fetchEntriesTo(begin, end, maxSize uint64, fetchSize *uint64, ents []eraftpb.Entry) []eraftpb.Entry {
	if begin >= end {
		return nil
	}
	y.Assert(ec.length() > 0)
	cacheLow := ec.front().Index
	y.Assert(begin >= cacheLow)
	cacheStart := int(begin - cacheLow)
	cacheEnd := int(end - cacheLow)
	if cacheEnd > ec.length() {
		cacheEnd = ec.length()
	}
	for i := cacheStart; i < cacheEnd; i++ {
		entry := ec.cache[i]
		y.AssertTruef(entry.Index == cacheLow+uint64(i), "%d %d %d", entry.Index, cacheLow, i)
		entrySize := uint64(entry.Size())
		*fetchSize += uint64(entrySize)
		if *fetchSize != entrySize && *fetchSize > maxSize {
			break
		}
		ents = append(ents, entry)
	}
	return ents
}

func (ec *EntryCache) append(tag string, entries []eraftpb.Entry) {
	if len(entries) == 0 {
		return
	}
	if ec.length() > 0 {
		firstIndex := entries[0].Index
		cacheLastIndex := ec.back().Index
		if cacheLastIndex >= firstIndex {
			if ec.front().Index >= firstIndex {
				ec.cache = ec.cache[:0]
			} else {
				left := ec.length() - int(cacheLastIndex-firstIndex+1)
				ec.cache = ec.cache[:left]
			}
		} else if cacheLastIndex+1 < firstIndex {
			panic(fmt.Sprintf("%s unexpected hole %d < %d", tag, cacheLastIndex, firstIndex))
		}
	}
	ec.cache = append(ec.cache, entries...)
	if ec.length() > MaxCacheCapacity {
		extraSize := ec.length() - MaxCacheCapacity
		ec.cache = ec.cache[extraSize:]
	}
}

func (ec *EntryCache) compactTo(idx uint64) {
	if ec.length() == 0 {
		return
	}
	firstIdx := ec.front().Index
	if firstIdx > idx {
		return
	}
	pos := mathutil.Min(int(idx-firstIdx), ec.length())
	ec.cache = ec.cache[pos:]
}

type ReadyApplySnapshot struct {
	// PrevRegion is the region before snapshot applied
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

type InvokeContext struct {
	Region     *metapb.Region
	RaftState  raftState
	ApplyState applyState
	lastTerm   uint64
	SnapData   *snapData
}

func NewInvokeContext(store *PeerStorage) *InvokeContext {
	ctx := &InvokeContext{
		Region:     store.region,
		RaftState:  store.raftState,
		ApplyState: store.applyState,
		lastTerm:   store.lastTerm,
	}
	return ctx
}

func (ic *InvokeContext) hasSnapshot() bool {
	return ic.SnapData != nil
}

func (ic *InvokeContext) saveRaftStateTo(wb *RaftWriteBatch) {
	key := y.KeyWithTs(RaftStateKey(ic.Region), RaftTS)
	wb.Set(key, ic.RaftState.Marshal())
}

func (ic *InvokeContext) saveApplyStateTo(wbs *KVWriteBatch) {
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	Engines *Engines

	peerID     uint64
	region     *metapb.Region
	raftState  raftState
	applyState applyState
	lastTerm   uint64

	snapState SnapState
	//genSnapTask  *GenSnapTask
	regionSched  chan<- task
	snapTriedCnt int

	cache *EntryCache
	stats *CacheQueryStats

	// stableApplyState is the applyState that is persisted to L0 file.
	stableApplyState applyState

	applyingChanges []*sdbpb.ChangeSet
	splitStage      sdbpb.SplitStage
	initialFlushed  bool

	Tag string
}

func NewPeerStorage(engines *Engines, region *metapb.Region, regionSched chan<- task, peerID uint64, tag string) (*PeerStorage, error) {
	log.S().Debugf("%s creating storage for %s", tag, region.String())
	raftState, err := initRaftState(engines.raft, region)
	if err != nil {
		return nil, err
	}
	applyState, err := initApplyState(engines.kv, region)
	if err != nil {
		return nil, err
	}
	if raftState.lastIndex < applyState.appliedIndex {
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.lastIndex, applyState.appliedIndex))
	}
	lastTerm, err := initLastTerm(engines.raft, region, raftState, applyState)
	if err != nil {
		return nil, err
	}
	var initialFlushed bool
	splitStage := sdbpb.SplitStage_INITIAL
	if shard := engines.kv.GetShard(region.Id); shard != nil {
		initialFlushed = shard.IsInitialFlushed()
		splitStage = shard.GetSplitStage()
	}
	return &PeerStorage{
		Engines:        engines,
		peerID:         peerID,
		region:         region,
		Tag:            tag,
		raftState:      raftState,
		applyState:     applyState,
		lastTerm:       lastTerm,
		regionSched:    regionSched,
		cache:          &EntryCache{},
		stats:          &CacheQueryStats{},
		splitStage:     splitStage,
		initialFlushed: initialFlushed,
	}, nil
}

func getMsg(engine *badger.DB, key []byte, msg proto.Message) error {
	val, err := getValue(engine, key)
	if err != nil {
		return err
	}
	return proto.Unmarshal(val, msg)
}

func getValue(engine *badger.DB, key []byte) ([]byte, error) {
	var result []byte
	err := engine.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		result = val
		return err
	})
	return result, err
}

func initRaftState(raftEngine *badger.DB, region *metapb.Region) (raftState, error) {
	stateKey := RaftStateKey(region)
	raftState := raftState{}
	val, err := getValue(raftEngine, stateKey)
	if err != nil && err != badger.ErrKeyNotFound {
		return raftState, err
	}
	if err == badger.ErrKeyNotFound {
		if len(region.Peers) > 0 {
			// new split region
			raftState.lastIndex = RaftInitLogIndex
			raftState.term = RaftInitLogTerm
			raftState.commit = RaftInitLogIndex
			wb := new(RaftWriteBatch)
			wb.Set(y.KeyWithTs(stateKey, RaftTS), raftState.Marshal())
			err = wb.WriteToRaft(raftEngine)
			if err != nil {
				return raftState, err
			}
			log.S().Infof("region %d:%d init raft state", region.Id, region.RegionEpoch.Version)
		}
	} else {
		raftState.Unmarshal(val)
	}
	return raftState, nil
}

func initApplyState(kvEngine *sdb.DB, region *metapb.Region) (applyState, error) {
	shard := kvEngine.GetShard(region.Id)
	applyState := applyState{}
	if shard != nil {
		val, ok := kvEngine.GetProperty(shard, applyStateKey)
		y.Assert(ok)
		y.Assert(len(val) == 32)
		applyState.Unmarshal(val)
	} else {
		if len(region.Peers) > 0 {
			applyState = newInitialApplyState()
		}
	}
	return applyState, nil
}

func initLastTerm(raftEngine *badger.DB, region *metapb.Region,
	raftState raftState, applyState applyState) (uint64, error) {
	lastIdx := raftState.lastIndex
	if lastIdx == 0 {
		return 0, nil
	} else if lastIdx == RaftInitLogIndex {
		return RaftInitLogTerm, nil
	} else if lastIdx == applyState.truncatedIndex {
		return applyState.truncatedTerm, nil
	} else {
		y.Assert(lastIdx > RaftInitLogIndex)
	}
	lastLogKey := RaftLogKey(region.Id, lastIdx)
	e := new(eraftpb.Entry)
	err := getMsg(raftEngine, lastLogKey, e)
	if err != nil {
		return 0, errors.Errorf("[region %s] entry at %d doesn't exist, may lost data.", region, lastIdx)
	}
	return e.Term, nil
}

func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raftState.commit == 0 && raftState.term == 0 && raftState.vote == 0 {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %s has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return eraftpb.HardState{
		Term:   raftState.term,
		Vote:   raftState.vote,
		Commit: raftState.commit,
	}, confStateFromRegion(ps.region), nil
}

func confStateFromRegion(region *metapb.Region) (confState eraftpb.ConfState) {
	for _, p := range region.Peers {
		if p.Role == metapb.PeerRole_Learner {
			confState.Learners = append(confState.Learners, p.GetId())
		} else {
			confState.Voters = append(confState.Voters, p.GetId())
		}
	}
	return
}

func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

func (ps *PeerStorage) IsApplyingSnapshot() bool {
	return ps.snapState.StateType == SnapState_Applying
}

func (ps *PeerStorage) Entries(low, high, maxSize uint64) ([]eraftpb.Entry, error) {
	err := ps.checkRange(low, high)
	if err != nil {
		return nil, err
	}
	ents := make([]eraftpb.Entry, 0, high-low)
	if low == high {
		return ents, nil
	}
	cacheLow := uint64(math.MaxUint64)
	if ps.cache.length() > 0 {
		cacheLow = ps.cache.front().Index
	}
	reginID := ps.region.Id
	if high <= cacheLow {
		// not overlap
		ps.stats.miss++
		ents, _, err = fetchEntriesTo(ps.Engines.raft, reginID, low, high, maxSize, ents)
		if err != nil {
			return ents, err
		}
		return ents, nil
	}
	var fetchedSize, beginIdx uint64
	if low < cacheLow {
		ps.stats.miss++
		ents, fetchedSize, err = fetchEntriesTo(ps.Engines.raft, reginID, low, cacheLow, maxSize, ents)
		if fetchedSize > maxSize {
			// maxSize exceed.
			return ents, nil
		}
		beginIdx = cacheLow
	} else {
		beginIdx = low
	}
	ps.stats.hit++
	return ps.cache.fetchEntriesTo(beginIdx, high, maxSize, &fetchedSize, ents), nil
}

func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	err := ps.checkRange(idx, idx+1)
	if err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.lastTerm || idx == ps.raftState.lastIndex {
		return ps.lastTerm, nil
	}
	entries, err := ps.Entries(idx, idx+1, math.MaxUint64)
	if err != nil {
		return 0, err
	}
	return entries[0].Term, nil
}

func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() {
		return raft.ErrCompacted
	} else if high > ps.raftState.lastIndex+1 {
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.lastIndex)
	}
	return nil
}

func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState.truncatedIndex
}

func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState.truncatedTerm
}

func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.lastIndex, nil
}

func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState.appliedIndex
}

func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return firstIndex(ps.applyState), nil
}

func firstIndex(applyState applyState) uint64 {
	return applyState.truncatedIndex + 1
}

func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.S().Infof("snapshot is stale, generate again, regionID: %d, peerID: %d, snapIndex: %d, truncatedIndex: %d", ps.region.GetId(), ps.peerID, idx, ps.truncatedIndex())
		return false
	}
	snapData := new(snapData)
	if err := snapData.Unmarshal(snap.GetData()); err != nil {
		log.S().Errorf("failed to decode snapshot, it may be corrupted, regionID: %d, peerID: %d, err: %v", ps.region.GetId(), ps.peerID, err)
		return false
	}
	snapEpoch := snapData.region.GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.S().Infof("snapshot epoch is stale, regionID: %d, peerID: %d, snapEpoch: %s, latestEpoch: %s", ps.region.GetId(), ps.peerID, snapEpoch, latestEpoch)
		return false
	}
	return true
}

func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	var snap eraftpb.Snapshot
	if !ps.initialFlushed {
		log.S().Infof("shard %d:%d has not flushed for generating snapshot", ps.region.Id, ps.region.RegionEpoch.Version)
		return snap, raft.ErrSnapshotTemporarilyUnavailable
	}
	if ps.snapState.StateType == SnapState_Generating {
		select {
		case s := <-ps.snapState.Receiver:
			snap = *s
		default:
			return snap, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = SnapState_Relax
		if snap.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snap) {
				return snap, nil
			}
		} else {
			log.S().Warnf("failed to try generating snapshot, regionID: %d, peerID: %d, times: %d", ps.region.GetId(), ps.peerID, ps.snapTriedCnt)
		}
	}
	if ps.snapTriedCnt >= MaxSnapRetryCnt {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snap, err
	}

	log.S().Infof("requesting snapshot, regionID: %d, peerID: %d", ps.region.GetId(), ps.peerID)
	ps.snapTriedCnt++
	ch := make(chan *eraftpb.Snapshot, 1)
	ps.snapState = SnapState{
		StateType: SnapState_Generating,
		Receiver:  ch,
	}
	// Schedule gen snapshot task directly instead of .
	ps.regionSched <- task{
		tp: taskTypeRegionGen,
		data: &regionTask{
			region:   ps.region,
			notifier: ch,
		},
	}
	return snap, raft.ErrSnapshotTemporarilyUnavailable
}

// Append the given entries to the raft log using previous last index or self.last_index.
// Return the new last index for later update. After we commit in engine, we can set last_index
// to the return one.
func (ps *PeerStorage) Append(invokeCtx *InvokeContext, entries []eraftpb.Entry, raftWB *RaftWriteBatch) error {
	log.S().Debugf("%s append %d entries", ps.Tag, len(entries))
	prevLastIndex := invokeCtx.RaftState.lastIndex
	if len(entries) == 0 {
		return nil
	}
	lastEntry := entries[len(entries)-1]
	lastIndex := lastEntry.Index
	lastTerm := lastEntry.Term
	for _, entry := range entries {
		err := raftWB.SetMsg(y.KeyWithTs(RaftLogKey(ps.region.Id, entry.Index), RaftTS), &entry)
		if err != nil {
			return err
		}
	}
	// Delete any previously appended log entries which never committed.
	for i := lastIndex + 1; i <= prevLastIndex; i++ {
		raftWB.Delete(y.KeyWithTs(RaftLogKey(ps.region.Id, i), RaftTS))
	}
	invokeCtx.RaftState.lastIndex = lastIndex
	invokeCtx.lastTerm = lastTerm

	// TODO: if the writebatch is failed to commit, the cache will be wrong.
	ps.cache.append(ps.Tag, entries)
	return nil
}

func (ps *PeerStorage) CompactTo(idx uint64) {
	ps.cache.compactTo(idx)
}

func (ps *PeerStorage) MaybeGCCache(replicatedIdx, appliedIdx uint64) {
	if replicatedIdx == appliedIdx {
		// The region is inactive, clear the cache immediately.
		ps.cache.compactTo(appliedIdx + 1)
	} else {
		if ps.cache.length() == 0 {
			return
		}
		cacheFirstIdx := ps.cache.front().Index
		if cacheFirstIdx > replicatedIdx+1 {
			// Catching up log requires accessing fs already, let's optimize for
			// the common case.
			// Maybe gc to second least replicated_idx is better.
			ps.cache.compactTo(appliedIdx + 1)
		}
	}
}

func (ps *PeerStorage) clearMeta(raftWB *RaftWriteBatch) error {
	return ClearMeta(ps.Engines, raftWB, ps.region, ps.raftState.lastIndex)
}

type CacheQueryStats struct {
	hit  uint64
	miss uint64
}

func fetchEntriesTo(engine *badger.DB, regionID, low, high, maxSize uint64, buf []eraftpb.Entry) ([]eraftpb.Entry, uint64, error) {
	var totalSize uint64
	nextIndex := low
	exceededMaxSize := false
	txn := engine.NewTransaction(false)
	defer txn.Discard()
	if high-low <= raftLogMultiGetCnt {
		// If election happens in inactive regions, they will just try
		// to fetch one empty log.
		for i := low; i < high; i++ {
			key := RaftLogKey(regionID, i)
			item, err := txn.Get(key)
			if err == badger.ErrKeyNotFound {
				log.S().Errorf("no enough entries, low %d high %d, idx %d", low, high, i)
				return nil, 0, raft.ErrUnavailable
			} else if err != nil {
				return nil, 0, err
			}
			val, err := item.Value()
			if err != nil {
				return nil, 0, err
			}
			var entry eraftpb.Entry
			err = entry.Unmarshal(val)
			if err != nil {
				return nil, 0, err
			}
			y.Assert(entry.Index == i)
			totalSize += uint64(len(val))

			if len(buf) == 0 || totalSize <= maxSize {
				buf = append(buf, entry)
			}
			if totalSize > maxSize {
				break
			}
		}
		return buf, totalSize, nil
	}
	startKey := RaftLogKey(regionID, low)
	endKey := RaftLogKey(regionID, high)
	iter := txn.NewIterator(badger.IteratorOptions{
		StartKey: y.KeyWithTs(startKey, math.MaxUint64),
		EndKey:   y.KeyWithTs(endKey, 0),
	})
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, 0, err
		}
		var entry eraftpb.Entry
		err = entry.Unmarshal(val)
		if err != nil {
			return nil, 0, err
		}
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		totalSize += uint64(len(val))
		exceededMaxSize = totalSize > maxSize
		if !exceededMaxSize || len(buf) == 0 {
			buf = append(buf, entry)
		}
		if exceededMaxSize {
			break
		}
	}
	// If we get the correct number of entries, returns,
	// or the total size almost exceeds max_size, returns.
	if len(buf) == int(high-low) || exceededMaxSize {
		return buf, totalSize, nil
	}
	// Here means we don't fetch enough entries.
	return nil, 0, raft.ErrUnavailable
}

func ClearMeta(engines *Engines, raftWB *RaftWriteBatch, region *metapb.Region, lastIndex uint64) error {
	regionID := region.Id
	start := time.Now()
	raftWB.Delete(y.KeyWithTs(RegionStateKey(region), RaftTS))
	firstIndex := lastIndex + 1
	beginLogKey := RaftLogKey(regionID, 0)
	endLogKey := RaftLogKey(regionID, firstIndex)
	err := engines.raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.Delete(y.KeyWithTs(RaftLogKey(regionID, i), RaftTS))
	}
	raftWB.Delete(y.KeyWithTs(RaftStateKey(region), RaftTS))
	log.S().Infof(
		"region %d:%d clear peer meta 1 raft key and %d raft logs, takes %v",
		regionID,
		region.RegionEpoch.Version,
		lastIndex+1-firstIndex,
		time.Since(start),
	)
	return nil
}

func WritePeerState(raftWB *RaftWriteBatch, region *metapb.Region, state rspb.PeerState, mergeState *rspb.MergeState) {
	regionState := new(rspb.RegionLocalState)
	regionState.State = state
	regionState.Region = region
	if mergeState != nil {
		regionState.MergeState = mergeState
	}
	data, _ := regionState.Marshal()
	raftWB.Set(y.KeyWithTs(RegionStateKey(region), RaftTS), data)
}

// Apply the peer with given snapshot.
func (ps *PeerStorage) ApplySnapshot(ctx *InvokeContext, snap *eraftpb.Snapshot, raftWB *RaftWriteBatch) error {
	log.S().Infof("%v begin to apply snapshot", ps.Tag)

	snapData := new(snapData)
	if err := snapData.Unmarshal(snap.Data); err != nil {
		return err
	}

	if snapData.region.Id != ps.region.Id {
		return fmt.Errorf("mismatch region id %v != %v", snapData.region.Id, ps.region.Id)
	}

	if ps.isInitialized() {
		// we can only delete the old data when the peer is initialized.
		if err := ps.clearMeta(raftWB); err != nil {
			return err
		}
	}

	WritePeerState(raftWB, snapData.region, rspb.PeerState_Applying, nil)

	lastIdx := snap.Metadata.Index

	ctx.RaftState.lastIndex = lastIdx
	ctx.lastTerm = snap.Metadata.Term
	ctx.ApplyState.appliedIndex = lastIdx

	// The snapshot only contains log which index > applied index, so
	// here the truncate state's (index, term) is in snapshot metadata.
	ctx.ApplyState.truncatedIndex = lastIdx
	ctx.ApplyState.truncatedTerm = snap.Metadata.Term

	log.S().Debugf("%v apply snapshot for region %v with state %v ok", ps.Tag, snapData.region, ctx.ApplyState)
	ctx.SnapData = snapData
	return nil
}

/// Save memory states to disk.
///
/// This function only write data to `ready_ctx`'s `WriteBatch`. It's caller's duty to write
/// it explicitly to disk. If it's flushed to disk successfully, `post_ready` should be called
/// to update the memory states properly.
/// Do not modify ready in this function, this is a requirement to advance the ready object properly later.
func (ps *PeerStorage) SaveReadyState(raftWB *RaftWriteBatch, ready *raft.Ready) (*InvokeContext, error) {
	ctx := NewInvokeContext(ps)
	if !raft.IsEmptySnap(&ready.Snapshot) {
		if err := ps.ApplySnapshot(ctx, &ready.Snapshot, raftWB); err != nil {
			return nil, err
		}
	}

	if len(ready.Entries) != 0 {
		if err := ps.Append(ctx, ready.Entries, raftWB); err != nil {
			return nil, err
		}
	}

	// Last index is 0 means the peer is created from raft message
	// and has not applied snapshot yet, so skip persistent hard state.
	if ctx.RaftState.lastIndex > 0 {
		if !raft.IsEmptyHardState(ready.HardState) {
			ctx.RaftState.commit = ready.HardState.Commit
			ctx.RaftState.term = ready.HardState.Term
			ctx.RaftState.vote = ready.HardState.Vote
		}
	}

	if ctx.RaftState != ps.raftState {
		ctx.saveRaftStateTo(raftWB)
	}

	return ctx, nil
}

func PeerEqual(l, r *metapb.Peer) bool {
	return l.Id == r.Id && l.StoreId == r.StoreId && l.Role == r.Role
}

func RegionEqual(l, r *metapb.Region) bool {
	if l == nil || r == nil {
		return false
	}
	return l.Id == r.Id && l.RegionEpoch.Version == r.RegionEpoch.Version && l.RegionEpoch.ConfVer == r.RegionEpoch.ConfVer
}

func (ps *PeerStorage) maybeScheduleApplySnapshot(ctx *InvokeContext) *ReadyApplySnapshot {
	// If we apply snapshot ok, we should update some infos like applied index too.
	if ctx.SnapData == nil {
		return nil
	}
	ps.ScheduleApplyingSnapshot(ctx.SnapData)
	prevRegion := ps.region
	ps.region = ctx.SnapData.region
	ctx.SnapData = nil
	return &ReadyApplySnapshot{
		PrevRegion: prevRegion,
		Region:     ps.region,
	}
}

// updateStates update the memory state after ready changes are flushed to disk successfully.
func (ps *PeerStorage) updateStates(ctx *InvokeContext) {
	ps.raftState = ctx.RaftState
	ps.applyState = ctx.ApplyState
	ps.lastTerm = ctx.lastTerm
}

func (ps *PeerStorage) ScheduleApplyingSnapshot(snapData *snapData) {
	status := JobStatus_Pending
	ps.snapState = SnapState{
		StateType: SnapState_Applying,
		Status:    &status,
	}
	ps.regionSched <- task{
		tp: taskTypeRegionApply,
		data: &regionTask{
			region:   ps.region,
			status:   &status,
			snapData: snapData,
		},
	}
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) ClearData() error {
	// Todo: currently it is a place holder
	return nil
}

func (p *PeerStorage) CancelApplyingSnap() bool {
	// Todo: currently it is a place holder
	return true
}

func (ps *PeerStorage) onGoingFlushCnt() int {
	var count int
	for _, change := range ps.applyingChanges {
		if change.Flush != nil {
			count++
		}
	}
	return count
}

func (ps *PeerStorage) hasOnGoingPreSplitFlush() bool {
	for _, change := range ps.applyingChanges {
		if change.Flush != nil {
			if change.Stage == sdbpb.SplitStage_PRE_SPLIT_FLUSH_DONE {
				return true
			}
		}
	}
	return false
}

// Check if the storage is applying a snapshot.
func (p *PeerStorage) CheckApplyingSnap() bool {
	switch p.snapState.StateType {
	case SnapState_Applying:
		switch atomic.LoadUint32(p.snapState.Status) {
		case JobStatus_Finished:
			p.snapState = SnapState{StateType: SnapState_Relax}
		case JobStatus_Cancelled:
			p.snapState = SnapState{StateType: SnapState_ApplyAborted}
		case JobStatus_Failed:
			panic(fmt.Sprintf("%v applying snapshot failed", p.Tag))
		default:
			return true
		}
	}
	return false
}

type snapData struct {
	region    *metapb.Region
	changeSet *sdbpb.ChangeSet
}

func (sd *snapData) Marshal() []byte {
	regionData, _ := sd.region.Marshal()
	changeData, _ := sd.changeSet.Marshal()
	buf := make([]byte, 0, 4+len(regionData)+4+len(changeData))
	buf = appendSlice(buf, regionData)
	buf = appendSlice(buf, changeData)
	return buf
}

func (sd *snapData) Unmarshal(data []byte) error {
	sd.region = new(metapb.Region)
	element, data := cutSlices(data)
	err := sd.region.Unmarshal(element)
	if err != nil {
		return err
	}
	sd.changeSet = new(sdbpb.ChangeSet)
	element, data = cutSlices(data)
	err = sd.changeSet.Unmarshal(element)
	if err != nil {
		return err
	}
	return nil
}

func appendSlice(buf []byte, element []byte) []byte {
	buf = append(buf, make([]byte, 4)...)
	binary.LittleEndian.PutUint32(buf[len(buf)-4:], uint32(len(element)))
	return append(buf, element...)
}

func appendU64(buf []byte, v uint64) []byte {
	tmp := make([]byte, 8)
	binary.LittleEndian.PutUint64(tmp, v)
	return append(buf, tmp...)
}

func cutSlices(data []byte) (element []byte, remain []byte) {
	length := binary.LittleEndian.Uint32(data)
	data = data[4:]
	return data[:length], data[length:]
}
