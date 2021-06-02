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
	"github.com/ngaut/unistore/raftengine"
	"github.com/ngaut/unistore/sdb"
	"github.com/ngaut/unistore/sdbpb"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/badger/y"
	"sync"
)

type Engines struct {
	kv       *sdb.DB
	kvPath   string
	raft     *raftengine.Engine
	raftPath string
	listener *MetaChangeListener
}

func NewEngines(kvEngine *sdb.DB, raftEngine *raftengine.Engine, kvPath, raftPath string, listener *MetaChangeListener) *Engines {
	kvEngine.IterateMeta(func(meta *sdb.ShardMeta) {
		if val, ok := meta.GetProperty(applyStateKey); ok {
			var applyState applyState
			applyState.Unmarshal(val)
		}
	})
	return &Engines{
		kv:       kvEngine,
		kvPath:   kvPath,
		raft:     raftEngine,
		raftPath: raftPath,
		listener: listener,
	}
}

func (en *Engines) WriteKV(wb *KVWriteBatch) {
	for _, batch := range wb.batches {
		en.kv.Write(batch)
	}
}

type KVWriteBatch struct {
	kv      *sdb.DB
	batches map[uint64]*sdb.WriteBatch
}

func NewKVWriteBatch(kv *sdb.DB) *KVWriteBatch {
	return &KVWriteBatch{
		kv:      kv,
		batches: map[uint64]*sdb.WriteBatch{},
	}
}

func (kvWB *KVWriteBatch) getEngineWriteBatch(regionID uint64) *sdb.WriteBatch {
	wb, ok := kvWB.batches[regionID]
	if !ok {
		wb = kvWB.kv.NewWriteBatch(kvWB.kv.GetShard(regionID))
		kvWB.batches[regionID] = wb
	}
	return wb
}

func SetLock(wb *sdb.WriteBatch, key, val []byte) {
	y.Assert(wb.Put(mvcc.LockCF, key, y.ValueStruct{Value: val}) == nil)
}

func DeleteLock(wb *sdb.WriteBatch, key []byte) {
	y.Assert(wb.Delete(mvcc.LockCF, key, 0) == nil)
}

func Rollback(wb *sdb.WriteBatch, key []byte, version uint64) {
	rollbackKey := mvcc.EncodeExtraTxnStatusKey(key, version)
	y.Assert(wb.Put(mvcc.ExtraCF, rollbackKey, y.ValueStruct{
		UserMeta: mvcc.NewDBUserMeta(version, 0),
		Version:  version,
	}) == nil)
}

func SetWithUserMeta(wb *sdb.WriteBatch, key, val, userMeta []byte, version uint64) {
	y.Assert(wb.Put(mvcc.WriteCF, key, y.ValueStruct{
		UserMeta: userMeta,
		Value:    val,
		Version:  version,
	}) == nil)
}

func SetOpLock(wb *sdb.WriteBatch, key, userMeta []byte, version uint64) {
	startTS := mvcc.DBUserMeta(userMeta).StartTS()
	opLockKey := mvcc.EncodeExtraTxnStatusKey(key, startTS)
	y.Assert(wb.Put(mvcc.ExtraCF, opLockKey, y.ValueStruct{UserMeta: userMeta, Version: version}) == nil)
}

func SetApplyState(wb *sdb.WriteBatch, state applyState) {
	wb.SetProperty(applyStateKey, state.Marshal())
}

func SetMaxMemTableSize(wb *sdb.WriteBatch, val []byte) {
	wb.SetProperty(sdb.MemTableSizeKey, val)
}

// MetaChangeListener implements the badger.MetaChangeListener interface.
type MetaChangeListener struct {
	mu    sync.Mutex
	msgCh chan<- Msg
	queue []Msg
}

func NewMetaChangeListener() *MetaChangeListener {
	return &MetaChangeListener{}
}

// OnChange implements the badger.MetaChangeListener interface.
func (l *MetaChangeListener) OnChange(e *sdbpb.ChangeSet) {
	y.Assert(e.ShardID != 0)
	msg := NewPeerMsg(MsgTypeGenerateEngineChangeSet, e.ShardID, e)
	l.mu.Lock()
	ch := l.msgCh
	if ch == nil {
		l.queue = append(l.queue, msg)
	}
	l.mu.Unlock()
	if ch != nil {
		ch <- msg
	}
}

func (l *MetaChangeListener) initMsgCh(msgCh chan<- Msg) {
	l.mu.Lock()
	l.msgCh = msgCh
	queue := l.queue
	l.mu.Unlock()
	for _, msg := range queue {
		msgCh <- msg
	}
}
