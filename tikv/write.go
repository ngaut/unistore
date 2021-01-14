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
	"math"
	"sync"
	"sync/atomic"

	"github.com/cznic/mathutil"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

const (
	batchChanSize = 1024
)

type writeDBWorker struct {
	batchCh chan *writeBatch
	writer  *dbWriter
}

func (w writeDBWorker) run() {
	defer w.writer.wg.Done()
	var batches []*writeBatch
	for {
		for i := range batches {
			batches[i] = nil
		}
		batches = batches[:0]
		select {
		case <-w.writer.closeCh:
			return
		case batch := <-w.batchCh:
			batches = append(batches, batch)
		}
		chLen := len(w.batchCh)
		for i := 0; i < chLen; i++ {
			batches = append(batches, <-w.batchCh)
		}
		if len(batches) > 0 {
			w.updateBatchGroup(batches)
		}
	}
}

func (w writeDBWorker) updateBatchGroup(batchGroup []*writeBatch) {
	wbMap := map[uint64]*badger.WriteBatch{}
	for _, batch := range batchGroup {
		wb, ok := wbMap[batch.regionID]
		if !ok {
			wb = w.writer.db.NewWriteBatch(w.writer.db.GetShard(batch.regionID))
			wbMap[batch.regionID] = wb
		}
		for _, entry := range batch.dbEntries {
			var err error
			if len(entry.UserMeta) == 0 {
				err = wb.Delete(writeCF, entry.Key.UserKey, entry.Key.Version)
			} else {
				err = wb.Put(writeCF, entry.Key.UserKey, y.ValueStruct{Value: entry.Value, UserMeta: entry.UserMeta, Version: entry.Key.Version})
			}
			y.Assert(err == nil)
		}
		for _, entry := range batch.lockEntries {
			var err error
			if len(entry.UserMeta) == 0 {
				err = wb.Delete(lockCF, entry.Key.UserKey, 0)
			} else {
				err = wb.Put(lockCF, entry.Key.UserKey, y.ValueStruct{Value: entry.Value, UserMeta: entry.UserMeta})
			}
			y.Assert(err == nil)
		}
		for _, entry := range batch.extraEntries {
			y.Assert(wb.Put(extraCF, entry.Key.UserKey, y.ValueStruct{UserMeta: entry.UserMeta, Version: entry.Key.Version}) == nil)
		}
	}
	wbs := make([]*badger.WriteBatch, 0, len(wbMap))
	for _, wb := range wbMap {
		wbs = append(wbs, wb)
	}
	e := w.writer.db.Write(wbs...)
	for _, batch := range batchGroup {
		batch.err = e
		batch.wg.Done()
	}
}

type dbWriter struct {
	db       *badger.ShardingDB
	dbCh     chan<- *writeBatch
	wg       sync.WaitGroup
	closeCh  chan struct{}
	latestTS uint64
}

func NewDBWriter(db *badger.ShardingDB) mvcc.DBWriter {
	return &dbWriter{
		db:      db,
		closeCh: make(chan struct{}, 0),
	}
}

func (writer *dbWriter) Open() {
	writer.wg.Add(1)

	dbCh := make(chan *writeBatch, batchChanSize)
	writer.dbCh = dbCh
	go writeDBWorker{
		batchCh: dbCh,
		writer:  writer,
	}.run()
}

func (writer *dbWriter) Close() {
	close(writer.closeCh)
	writer.wg.Wait()
}

func (writer *dbWriter) Write(batch mvcc.WriteBatch) error {
	wb := batch.(*writeBatch)
	wb.wg.Add(1)
	writer.dbCh <- wb
	wb.wg.Wait()
	return wb.err
}

type writeBatch struct {
	regionID     uint64
	startTS      uint64
	commitTS     uint64
	dbEntries    []*badger.Entry
	lockEntries  []*badger.Entry
	extraEntries []*badger.Entry
	wg           sync.WaitGroup
	err          error
}

func (wb *writeBatch) Prewrite(key []byte, lock *mvcc.MvccLock) {
	wb.putLock(key, lock)
}

func (wb *writeBatch) putLock(key []byte, lock *mvcc.MvccLock) {
	entry := &badger.Entry{
		Key:      y.KeyWithTs(key, 0),
		UserMeta: mvcc.LockUserMetaNone,
		Value:    lock.MarshalBinary(),
	}
	wb.lockEntries = append(wb.lockEntries, entry)
}

func (wb *writeBatch) delLock(key []byte) {
	entry := &badger.Entry{
		Key: y.KeyWithTs(key, 0),
	}
	entry.SetDelete()
	wb.lockEntries = append(wb.lockEntries, entry)
}

func (wb *writeBatch) Commit(key []byte, lock *mvcc.MvccLock) {
	userMeta := mvcc.NewDBUserMeta(wb.startTS, wb.commitTS)
	k := y.KeyWithTs(key, wb.commitTS)
	if lock.Op != uint8(kvrpcpb.Op_Lock) {
		wb.putData(k, lock.Value, userMeta)
	} else if bytes.Equal(key, lock.Primary) {
		opLockKey := y.KeyWithTs(mvcc.EncodeExtraTxnStatusKey(key, wb.startTS), wb.startTS)
		wb.putExtra(opLockKey, userMeta)
	}
	wb.delLock(key)
}

func (wb *writeBatch) putData(key y.Key, val, userMeta []byte) {
	wb.dbEntries = append(wb.dbEntries, &badger.Entry{
		Key:      key,
		Value:    val,
		UserMeta: userMeta,
	})
}

func (wb *writeBatch) putExtra(key y.Key, userMeta []byte) {
	wb.extraEntries = append(wb.extraEntries, &badger.Entry{
		Key:      key,
		UserMeta: userMeta,
	})
}

func (wb *writeBatch) Rollback(key []byte, deleteLock bool) {
	rollbackKey := y.KeyWithTs(mvcc.EncodeExtraTxnStatusKey(key, wb.startTS), wb.startTS)
	userMeta := mvcc.NewDBUserMeta(wb.startTS, 0)
	wb.putExtra(rollbackKey, userMeta)
	if deleteLock {
		wb.delLock(key)
	}
}

func (wb *writeBatch) PessimisticLock(key []byte, lock *mvcc.MvccLock) {
	wb.putLock(key, lock)
}

func (wb *writeBatch) PessimisticRollback(key []byte) {
	wb.delLock(key)
}

func (writer *dbWriter) NewWriteBatch(startTS, commitTS uint64, ctx *kvrpcpb.Context) mvcc.WriteBatch {
	if commitTS > 0 {
		writer.updateLatestTS(commitTS)
	} else {
		writer.updateLatestTS(startTS)
	}
	return &writeBatch{
		regionID: ctx.RegionId,
		startTS:  startTS,
		commitTS: commitTS,
	}
}

func (writer *dbWriter) getLatestTS() uint64 {
	return atomic.LoadUint64(&writer.latestTS)
}

func (writer *dbWriter) updateLatestTS(ts uint64) {
	latestTS := writer.getLatestTS()
	if ts != math.MaxUint64 && ts > latestTS {
		atomic.CompareAndSwapUint64(&writer.latestTS, latestTS, ts)
	}
}

func (writer *dbWriter) DeleteRange(startKey, endKey []byte, latchHandle mvcc.LatchHandle) error {
	// TODO
	return nil
}

func (writer *dbWriter) collectRangeKeys(it *badger.Iterator, startKey, endKey []byte, keys []y.Key) []y.Key {
	if len(endKey) == 0 {
		panic("invalid end key")
	}
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		if exceedEndKey(key, endKey) {
			break
		}
		keys = append(keys, y.KeyWithTs(key, item.Version()))
	}
	return keys
}

func (writer *dbWriter) deleteKeysInBatch(latchHandle mvcc.LatchHandle, keys []y.Key, batchSize int) error {
	for len(keys) > 0 {
		batchSize := mathutil.Min(len(keys), batchSize)
		batchKeys := keys[:batchSize]
		keys = keys[batchSize:]
		hashVals := userKeysToHashVals(batchKeys...)
		wb := &writeBatch{}
		for _, key := range batchKeys {
			key.Version++
			wb.dbEntries = append(wb.dbEntries, &badger.Entry{Key: key})
		}
		latchHandle.AcquireLatches(hashVals)
		wb.wg.Add(1)
		writer.dbCh <- wb
		wb.wg.Wait()
		latchHandle.ReleaseLatches(hashVals)
		if wb.err != nil {
			return wb.err
		}
	}
	return nil
}
