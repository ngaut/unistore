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
	"math"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ngaut/unistore/metrics"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
)

type regionSnapshot struct {
	regionState *raft_serverpb.RegionLocalState
	dbSnap      *badger.Snapshot
	term        uint64
	index       uint64
}

type Engines struct {
	kv       *badger.ShardingDB
	kvPath   string
	raft     *badger.DB
	raftPath string
	listener *MetaChangeListener
}

func NewEngines(kvEngine *badger.ShardingDB, raftEngine *badger.DB, kvPath, raftPath string, listener *MetaChangeListener) *Engines {
	return &Engines{
		kv:       kvEngine,
		kvPath:   kvPath,
		raft:     raftEngine,
		raftPath: raftPath,
		listener: listener,
	}
}

/*
func (en *Engines) newRegionSnapshot(task *regionTask) (snap *regionSnapshot, err error) {
	// We need to get the old region state out of the snapshot transaction to fetch data in lockStore.
	// The lockStore data must be fetch before we start the snapshot transaction to make sure there is no newer data
	// in the lockStore. The missing old data can be restored by raft log.
	oldRegionState, err := getRegionLocalState(en.kv, task.region)
	if err != nil {
		return nil, err
	}

	dbSnap := en.kv.NewSnapshot(en.kv.GetShard(task.region.Id))
	defer func() {
		if err != nil {
			dbSnap.Discard()
		}
	}()

	// Verify that the region version to make sure the start key and end key has not changed.
	regionState := new(raft_serverpb.RegionLocalState)
	val, err := getKVValueBySnap(dbSnap, RegionStateKey(task.region))
	if err != nil {
		return nil, err
	}
	err = regionState.Unmarshal(val)
	if err != nil {
		return nil, err
	}
	if regionState.Region.RegionEpoch.Version != oldRegionState.Region.RegionEpoch.Version {
		return nil, errors.New("region changed during newRegionSnapshot")
	}

	index, term, err := getAppliedIdxTermForSnapshot(en.raft, dbSnap, task.region.Id)
	if err != nil {
		return nil, err
	}
	snap = &regionSnapshot{
		regionState: regionState,
		dbSnap:      dbSnap,
		term:        term,
		index:       index,
	}
	return snap, nil
}
*/

func (en *Engines) WriteKV(wb *KVWriteBatch) error {
	return wb.WriteToEngine()
}

func (en *Engines) WriteRaft(wb *RaftWriteBatch) error {
	return wb.WriteToRaft(en.raft)
}

func (en *Engines) SyncKVWAL() error {
	// TODO: implement
	return nil
}

func (en *Engines) SyncRaftWAL() error {
	// TODO: implement
	return nil
}

type KVWriteBatch struct {
	kv      *badger.ShardingDB
	batches map[uint64]*badger.WriteBatch
}

func NewKVWriteBatch(kv *badger.ShardingDB) *KVWriteBatch {
	return &KVWriteBatch{
		kv:      kv,
		batches: map[uint64]*badger.WriteBatch{},
	}
}

func (kvWB *KVWriteBatch) getEngineWriteBatch(regionID uint64) *badger.WriteBatch {
	wb, ok := kvWB.batches[regionID]
	if !ok {
		wb = kvWB.kv.NewWriteBatch(kvWB.kv.GetShard(regionID))
		kvWB.batches[regionID] = wb
	}
	return wb
}

func (kvWB *KVWriteBatch) SetLock(regionID uint64, key, val []byte) {
	wb := kvWB.getEngineWriteBatch(regionID)
	y.Assert(wb.Put(mvcc.LockCF, key, y.ValueStruct{Value: val}) == nil)
}

func (kvWB *KVWriteBatch) DeleteLock(regionID uint64, key []byte) {
	wb := kvWB.getEngineWriteBatch(regionID)
	y.Assert(wb.Delete(mvcc.LockCF, key, 0) == nil)
}

func (kvWB *KVWriteBatch) Rollback(regionID uint64, key y.Key) {
	wb := kvWB.getEngineWriteBatch(regionID)
	rollbackKey := mvcc.EncodeExtraTxnStatusKey(key.UserKey, key.Version)
	y.Assert(wb.Put(mvcc.ExtraCF, rollbackKey, y.ValueStruct{
		UserMeta: mvcc.NewDBUserMeta(key.Version, 0),
		Version:  key.Version,
	}) == nil)
}

func (kvWB *KVWriteBatch) SetWithUserMeta(regionID uint64, key y.Key, val, userMeta []byte) {
	wb := kvWB.getEngineWriteBatch(regionID)
	y.Assert(wb.Put(mvcc.WriteCF, key.UserKey, y.ValueStruct{
		UserMeta: userMeta,
		Value:    val,
		Version:  key.Version,
	}) == nil)
}

func (kvWB *KVWriteBatch) SetOpLock(regionID uint64, key y.Key, userMeta []byte) {
	wb := kvWB.getEngineWriteBatch(regionID)
	startTS := mvcc.DBUserMeta(userMeta).StartTS()
	opLockKey := mvcc.EncodeExtraTxnStatusKey(key.UserKey, startTS)
	y.Assert(wb.Put(mvcc.ExtraCF, opLockKey, y.ValueStruct{UserMeta: userMeta, Version: key.Version}) == nil)
}

func (kvWB *KVWriteBatch) SetApplyState(regionID uint64, state applyState) {
	wb := kvWB.getEngineWriteBatch(regionID)
	wb.SetProperty(applyStateKey, state.Marshal())
}

func (kvWB *KVWriteBatch) WriteToEngine() error {
	batches := make([]*badger.WriteBatch, 0, len(kvWB.batches))
	for _, wb := range kvWB.batches {
		batches = append(batches, wb)
	}
	return kvWB.kv.Write(batches...)
}

func (kvWB *KVWriteBatch) Reset() {
	kvWB.batches = make(map[uint64]*badger.WriteBatch, len(kvWB.batches))
}

type RaftWriteBatch struct {
	entries       []*badger.Entry
	lockEntries   []*badger.Entry
	extraEntries  []*badger.Entry
	raftEntries   []*badger.Entry
	applyState    applyState
	size          int
	safePoint     int
	safePointLock int
	safePointSize int
	safePointUndo int
}

func (wb *RaftWriteBatch) Len() int {
	return len(wb.entries) + len(wb.lockEntries) + len(wb.extraEntries) + len(wb.raftEntries)
}

func (wb *RaftWriteBatch) Set(key y.Key, val []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key:   key,
		Value: val,
	})
	wb.size += key.Len() + len(val)
}

func (wb *RaftWriteBatch) Delete(key y.Key) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key: key,
	})
	wb.size += key.Len()
}

func (wb *RaftWriteBatch) SetMsg(key y.Key, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	wb.Set(key, val)
	return nil
}

func (wb *RaftWriteBatch) SetRegionLocalState(key y.Key, state *raft_serverpb.RegionLocalState) error {
	data, err := state.Marshal()
	if err != nil {
		return errors.WithStack(err)
	}
	wb.Set(key, data)
	return nil
}

func (wb *RaftWriteBatch) WriteToRaft(db *badger.DB) error {
	if len(wb.entries) > 0 {
		start := time.Now()
		err := db.Update(func(txn *badger.Txn) error {
			for _, entry := range wb.entries {
				if len(entry.Value) == 0 {
					entry.SetDelete()
				}
				err1 := txn.SetEntry(entry)
				if err1 != nil {
					return err1
				}
			}
			return nil
		})
		metrics.RaftDBUpdate.Observe(time.Since(start).Seconds())
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (wb *RaftWriteBatch) MustWriteToRaft(db *badger.DB) {
	err := wb.WriteToRaft(db)
	if err != nil {
		panic(err)
	}
}

func (wb *RaftWriteBatch) Reset() {
	for i := range wb.entries {
		wb.entries[i] = nil
	}
	wb.entries = wb.entries[:0]
	for i := range wb.lockEntries {
		wb.lockEntries[i] = nil
	}
	wb.lockEntries = wb.lockEntries[:0]
	for i := range wb.extraEntries {
		wb.extraEntries[i] = nil
	}
	wb.extraEntries = wb.extraEntries[:0]
	for i := range wb.raftEntries {
		wb.raftEntries[i] = nil
	}
	wb.raftEntries = wb.raftEntries[:0]
	wb.size = 0
	wb.safePoint = 0
	wb.safePointLock = 0
	wb.safePointSize = 0
	wb.safePointUndo = 0
}

const maxSystemTS = math.MaxUint64

type raftLogFilter struct {
}

func (r *raftLogFilter) Filter(key, val, userMeta []byte) badger.Decision {
	return badger.DecisionKeep
}

var raftLogGuard = badger.Guard{
	Prefix:   []byte{LocalPrefix, RaftStatePrefix},
	MatchLen: 10,
	MinSize:  1024 * 1024,
}

func (r *raftLogFilter) Guards() []badger.Guard {
	return []badger.Guard{
		raftLogGuard,
	}
}

func CreateRaftLogCompactionFilter(targetLevel int, startKey, endKey []byte) badger.CompactionFilter {
	return &raftLogFilter{}
}
