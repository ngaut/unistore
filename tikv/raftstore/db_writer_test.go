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
	"github.com/pingcap/badger/y"
	"github.com/pingcap/kvproto/pkg/metapb"
	"testing"

	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
)

func TestRaftWriteBatch_PrewriteAndCommit(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)
	apply := new(applier)
	apply.region = newBootstrapRegion(1, 1, 1)
	applyCtx := newApplyContext("test", nil, engines, nil, NewDefaultConfig())
	wb := newTestWritBatch(100, 0)
	// Testing PreWriter

	longValue := [128]byte{101}

	values := [][]byte{
		[]byte("short value"),
		longValue[:],
		[]byte(""),
	}

	for i := 0; i < 3; i++ {
		primary := []byte(fmt.Sprintf("t%08d_r%08d", i, i))
		expectLock := mvcc.MvccLock{
			MvccLockHdr: mvcc.MvccLockHdr{
				StartTS:    100,
				TTL:        10,
				Op:         uint8(kvrpcpb.Op_Put),
				PrimaryLen: uint16(len(primary)),
			},
			Primary: primary,
			Value:   values[i],
		}
		wb.Prewrite(primary, &expectLock)
		apply.execWriteCmd(applyCtx, wb.builder.Build())
		applyCtx.writeToDB()
		wb = newTestWritBatch(100, 0)
		snap := engines.kv.NewSnapshot(engines.kv.GetShard(apply.region.Id))
		item, err := snap.Get(mvcc.LockCF, y.KeyWithTs(primary, 0))
		assert.Nil(t, err)
		val, _ := item.Value()
		assert.NotNil(t, val)
		lock := mvcc.DecodeLock(val)
		assert.Equal(t, expectLock, lock)
		snap.Discard()
	}

	// Testing Commit
	wb = newTestWritBatch(100, 200)
	for i := 0; i < 3; i++ {
		primary := []byte(fmt.Sprintf("t%08d_r%08d", i, i))
		expectLock := &mvcc.MvccLock{
			MvccLockHdr: mvcc.MvccLockHdr{
				StartTS: 100,
				TTL:     10,
				Op:      uint8(mvcc.LockTypePut),
			},
			Value: values[i],
		}
		wb.Commit(primary, expectLock)
		apply.execWriteCmd(applyCtx, wb.builder.Build())
		applyCtx.writeToDB()
		wb = newTestWritBatch(100, 200)
		snap := engines.kv.NewSnapshot(engines.kv.GetShard(apply.region.Id))
		item, err := snap.Get(mvcc.WriteCF, y.KeyWithTs(primary, wb.commitTS))
		assert.Nil(t, err)
		curVal, err := item.Value()
		assert.Nil(t, err)
		assert.NotNil(t, item)
		userMeta := mvcc.DBUserMeta(item.UserMeta())
		assert.Equal(t, userMeta.StartTS(), expectLock.StartTS)
		assert.Equal(t, userMeta.CommitTS(), wb.commitTS)
		assert.Equal(t, 0, bytes.Compare(curVal, expectLock.Value))
		snap.Discard()
	}
}

func TestRaftWriteBatch_Rollback(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)
	apply := new(applier)
	apply.region = newBootstrapRegion(1, 1, 1)
	applyCtx := newApplyContext("test", nil, engines, nil, NewDefaultConfig())
	wb := newTestWritBatch(100, 0)

	longValue := [128]byte{102}

	for i := 0; i < 2; i++ {
		primary := []byte(fmt.Sprintf("t%08d_r%08d", i, i))
		expectLock := mvcc.MvccLock{
			MvccLockHdr: mvcc.MvccLockHdr{
				StartTS:    100,
				TTL:        10,
				Op:         uint8(kvrpcpb.Op_Put),
				PrimaryLen: uint16(len(primary)),
			},
			Primary: primary,
			Value:   longValue[:],
		}
		wb.Prewrite(primary, &expectLock)
		apply.execWriteCmd(applyCtx, wb.builder.Build())
		applyCtx.flush()
		wb = newTestWritBatch(100, 0)
	}

	// Testing RollBack
	wb = newTestWritBatch(150, 200)
	primary := []byte(fmt.Sprintf("t%08d_r%08d", 0, 0))
	wb.Rollback(primary, false)
	apply.execWriteCmd(applyCtx, wb.builder.Build())
	applyCtx.flush()

	wb = newTestWritBatch(100, 0)
	primary = []byte(fmt.Sprintf("t%08d_r%08d", 1, 1))
	wb.Rollback(primary, true)
	apply.execWriteCmd(applyCtx, wb.builder.Build())
	applyCtx.writeToDB()
	// The lock should be deleted.
	snap := engines.kv.NewSnapshot(engines.kv.GetShard(apply.region.Id))
	item, _ := snap.Get(mvcc.LockCF, y.KeyWithTs(primary, 0))
	assert.Nil(t, item)
}

func newTestWritBatch(startTS, commitTS uint64) *customWriteBatch {
	return NewCustomWriteBatch(startTS, commitTS, &kvrpcpb.Context{
		RegionId:    1,
		RegionEpoch: &metapb.RegionEpoch{Version: 1, ConfVer: 1},
		Peer:        &metapb.Peer{Id: 1, StoreId: 1},
	}).(*customWriteBatch)
}
