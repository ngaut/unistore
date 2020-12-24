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
	"testing"

	"github.com/ngaut/unistore/tikv/raftstore/raftlog"

	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rfpb "github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/stretchr/testify/assert"
)

func TestRaftWriteBatch_PrewriteAndCommit(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)
	apply := new(applier)
	applyCtx := newApplyContext("test", nil, engines, nil, nil, NewDefaultConfig())
	wb := &raftWriteBatch{
		startTS:  100,
		commitTS: 0,
	}
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
		apply.execWriteCmd(applyCtx, raftlog.NewRequest(&rfpb.RaftCmdRequest{
			Header:   new(rfpb.RaftRequestHeader),
			Requests: wb.requests,
		}))
		err := applyCtx.wb.WriteToKV(engines.kv)
		assert.Nil(t, err)
		applyCtx.wb.Reset()
		wb.requests = nil
		snap := engines.kv.NewSnapshot(primary, primary)
		item, err := snap.Get(mvcc.LockCF, y.KeyWithTs(primary, 0))
		assert.Nil(t, err)
		val, _ := item.Value()
		assert.NotNil(t, val)
		lock := mvcc.DecodeLock(val)
		assert.Equal(t, expectLock, lock)
		snap.Discard()
	}

	// Testing Commit
	wb = &raftWriteBatch{
		startTS:  100,
		commitTS: 200,
	}
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
		apply.execWriteCmd(applyCtx, raftlog.NewRequest(&rfpb.RaftCmdRequest{
			Header:   new(rfpb.RaftRequestHeader),
			Requests: wb.requests,
		}))
		err := applyCtx.wb.WriteToKV(engines.kv)
		assert.Nil(t, err)
		applyCtx.wb.Reset()
		wb.requests = nil
		snap := engines.kv.NewSnapshot(primary, primary)
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
	applyCtx := newApplyContext("test", nil, engines, nil, nil, NewDefaultConfig())
	wb := &raftWriteBatch{
		startTS:  100,
		commitTS: 0,
	}

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
		apply.execWriteCmd(applyCtx, raftlog.NewRequest(&rfpb.RaftCmdRequest{
			Header:   new(rfpb.RaftRequestHeader),
			Requests: wb.requests,
		}))
		err := applyCtx.wb.WriteToKV(engines.kv)
		assert.Nil(t, err)
		applyCtx.wb.Reset()
		wb.requests = nil
	}

	// Testing RollBack
	wb = &raftWriteBatch{
		startTS:  150,
		commitTS: 200,
	}
	primary := []byte(fmt.Sprintf("t%08d_r%08d", 0, 0))
	wb.Rollback(primary, false)
	apply.execWriteCmd(applyCtx, raftlog.NewRequest(&rfpb.RaftCmdRequest{
		Header:   new(rfpb.RaftRequestHeader),
		Requests: wb.requests,
	}))
	err := applyCtx.wb.WriteToKV(engines.kv)
	assert.Nil(t, err)
	applyCtx.wb.Reset()

	wb = &raftWriteBatch{
		startTS:  100,
		commitTS: 200,
	}
	primary = []byte(fmt.Sprintf("t%08d_r%08d", 1, 1))
	wb.Rollback(primary, true)
	apply.execWriteCmd(applyCtx, raftlog.NewRequest(&rfpb.RaftCmdRequest{
		Header:   new(rfpb.RaftRequestHeader),
		Requests: wb.requests,
	}))
	err = applyCtx.wb.WriteToKV(engines.kv)
	assert.Nil(t, err)
	applyCtx.wb.Reset()
	// The lock should be deleted.
	snap := engines.kv.NewSnapshot(primary, primary)
	item, _ := snap.Get(mvcc.LockCF, y.KeyWithTs(primary, 0))
	assert.Nil(t, item)
}
