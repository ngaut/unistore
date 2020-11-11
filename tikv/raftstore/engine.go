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
}

func NewEngines(kvEngine *badger.ShardingDB, raftEngine *badger.DB, kvPath, raftPath string) *Engines {
	return &Engines{
		kv:       kvEngine,
		kvPath:   kvPath,
		raft:     raftEngine,
		raftPath: raftPath,
	}
}

func (en *Engines) newRegionSnapshot(task *regionTask) (snap *regionSnapshot, err error) {
	// We need to get the old region state out of the snapshot transaction to fetch data in lockStore.
	// The lockStore data must be fetch before we start the snapshot transaction to make sure there is no newer data
	// in the lockStore. The missing old data can be restored by raft log.
	oldRegionState, err := getRegionLocalState(en.kv, task.regionId)
	if err != nil {
		return nil, err
	}

	dbSnap := en.kv.NewSnapshot(task.startKey, task.endKey)
	defer func() {
		if err != nil {
			dbSnap.Discard()
		}
	}()

	// Verify that the region version to make sure the start key and end key has not changed.
	regionState := new(raft_serverpb.RegionLocalState)
	val, err := getKVValueBySnap(dbSnap, RegionStateKey(task.regionId))
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

	index, term, err := getAppliedIdxTermForSnapshot(en.raft, dbSnap, task.regionId)
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

func (en *Engines) WriteKV(wb *WriteBatch) error {
	return wb.WriteToKV(en.kv)
}

func (en *Engines) WriteRaft(wb *WriteBatch) error {
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

type WriteBatch struct {
	entries       []*badger.Entry
	lockEntries   []*badger.Entry
	extraEntries  []*badger.Entry
	raftEntries   []*badger.Entry
	size          int
	safePoint     int
	safePointLock int
	safePointSize int
	safePointUndo int
}

func (wb *WriteBatch) Len() int {
	return len(wb.entries) + len(wb.lockEntries) + len(wb.extraEntries) + len(wb.raftEntries)
}

func (wb *WriteBatch) Set(key y.Key, val []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key:   key,
		Value: val,
	})
	wb.size += key.Len() + len(val)
}

func (wb *WriteBatch) SetLock(key, val []byte) {
	wb.lockEntries = append(wb.lockEntries, &badger.Entry{
		Key:   y.KeyWithTs(key, 0),
		Value: val,
	})
	wb.size += len(key) + len(val)
}

func (wb *WriteBatch) DeleteLock(key []byte) {
	wb.lockEntries = append(wb.lockEntries, &badger.Entry{
		Key: y.KeyWithTs(key, 0),
	})
	wb.size += len(key)
}

func (wb *WriteBatch) Rollback(key y.Key) {
	rollbackKey := mvcc.EncodeExtraTxnStatusKey(key.UserKey, key.Version)
	wb.extraEntries = append(wb.extraEntries, &badger.Entry{
		Key:      y.KeyWithTs(rollbackKey, key.Version),
		UserMeta: mvcc.NewDBUserMeta(key.Version, 0),
	})
	wb.size += len(rollbackKey)
}

func (wb *WriteBatch) SetWithUserMeta(key y.Key, val, userMeta []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key:      key,
		Value:    val,
		UserMeta: userMeta,
	})
	wb.size += key.Len() + len(val) + len(userMeta)
}

func (wb *WriteBatch) SetOpLock(key y.Key, userMeta []byte) {
	startTS := mvcc.DBUserMeta(userMeta).StartTS()
	opLockKey := y.KeyWithTs(mvcc.EncodeExtraTxnStatusKey(key.UserKey, startTS), key.Version)
	e := &badger.Entry{
		Key:      opLockKey,
		UserMeta: userMeta,
	}
	wb.extraEntries = append(wb.extraEntries, e)
	wb.size += key.Len() + len(userMeta)
}

func (wb *WriteBatch) Delete(key y.Key) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key: key,
	})
	wb.size += key.Len()
}

func (wb *WriteBatch) SetMsg(key y.Key, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	wb.Set(key, val)
	return nil
}

func (wb *WriteBatch) SetApplyState(key y.Key, applyState applyState) {
	wb.SetRaftCF(key, applyState.Marshal())
}

func (wb *WriteBatch) SetRegionLocalState(key y.Key, state *raft_serverpb.RegionLocalState) error {
	data, err := state.Marshal()
	if err != nil {
		return errors.WithStack(err)
	}
	wb.SetRaftCF(key, data)
	return nil
}

func (wb *WriteBatch) SetRaftCF(key y.Key, val []byte) {
	wb.raftEntries = append(wb.raftEntries, &badger.Entry{
		Key:   key,
		Value: val,
	})
	wb.size += len(key.UserKey) + len(val)
}

func (wb *WriteBatch) SetSafePoint() {
	wb.safePoint = len(wb.entries)
	wb.safePointLock = len(wb.lockEntries)
	wb.safePointSize = wb.size
}

func (wb *WriteBatch) RollbackToSafePoint() {
	wb.entries = wb.entries[:wb.safePoint]
	wb.lockEntries = wb.lockEntries[:wb.safePointLock]
	wb.size = wb.safePointSize
}

// WriteToKV flush WriteBatch to DB by two steps:
// 	1. Write entries to badger. After save ApplyState to badger, subsequent regionSnapshot will start at new raft index.
//	2. Update lockStore, the date in lockStore may be older than the DB, so we need to restore then entries from raft log.
func (wb *WriteBatch) WriteToKV(db *badger.ShardingDB) error {
	dbWB := db.NewWriteBatch()
	start := time.Now()
	if len(wb.entries) > 0 {
		for _, entry := range wb.entries {
			if len(entry.UserMeta) == 0 && len(entry.Value) == 0 {
				y.Assert(dbWB.Delete(mvcc.WriteCF, entry.Key.UserKey, entry.Key.Version) == nil)
			} else {
				val := y.ValueStruct{
					Value:    entry.Value,
					UserMeta: entry.UserMeta,
					Version:  entry.Key.Version,
				}
				y.Assert(dbWB.Put(mvcc.WriteCF, entry.Key.UserKey, val) == nil)
			}
		}
	}
	if len(wb.lockEntries) > 0 {
		for _, entry := range wb.lockEntries {
			if len(entry.Value) == 0 {
				y.Assert(dbWB.Delete(mvcc.LockCF, entry.Key.UserKey, 0) == nil)
			} else {
				y.Assert(dbWB.Put(mvcc.LockCF, entry.Key.UserKey, y.ValueStruct{Value: entry.Value}) == nil)
			}
		}
	}
	if len(wb.extraEntries) > 0 {
		for _, entry := range wb.extraEntries {
			if len(entry.UserMeta) == 0 {
				y.Assert(dbWB.Delete(mvcc.ExtraCF, entry.Key.UserKey, entry.Key.Version) == nil)
			} else {
				val := y.ValueStruct{
					Value:    entry.Value,
					UserMeta: entry.UserMeta,
					Version:  entry.Key.Version,
				}
				y.Assert(dbWB.Put(mvcc.ExtraCF, entry.Key.UserKey, val) == nil)
			}
		}
	}
	if len(wb.raftEntries) > 0 {
		for _, entry := range wb.raftEntries {
			if len(entry.Value) == 0 {
				y.Assert(dbWB.Delete(mvcc.RaftCF, entry.Key.UserKey, 0) == nil)
			} else {
				y.Assert(dbWB.Put(mvcc.RaftCF, entry.Key.UserKey, y.ValueStruct{Value: entry.Value}) == nil)
			}
		}
	}
	err := db.Write(dbWB)
	metrics.KVDBUpdate.Observe(time.Since(start).Seconds())
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (wb *WriteBatch) WriteToRaft(db *badger.DB) error {
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

func (wb *WriteBatch) MustWriteToKV(db *badger.ShardingDB) {
	err := wb.WriteToKV(db)
	if err != nil {
		panic(err)
	}
}

func (wb *WriteBatch) MustWriteToRaft(db *badger.DB) {
	err := wb.WriteToRaft(db)
	if err != nil {
		panic(err)
	}
}

func (wb *WriteBatch) Reset() {
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
	Prefix:   []byte{LocalPrefix, RegionRaftPrefix},
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
