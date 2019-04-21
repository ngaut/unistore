package raftstore

import (
	"fmt"
	"github.com/coocood/badger"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rfpb "github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRaftWriteBatch_PrewriteAndCommit(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)
	apply := new(applyDelegate)
	applyCtx := newApplyContext("test", nil, engines, nil, notifier{}, NewDefaultConfig())
	dBWriter := new(raftDBWriter)
	wb := (dBWriter.NewWriteBatch(100, 0)).(*raftWriteBatch)
	// Testing PreWriter

	values := [][]byte{
		[]byte("short value"),
		[]byte("large value .........."),
		[]byte("s"),
	}

	for i := 0; i < 3; i++ {
		expectKey := []byte(fmt.Sprintf("%08d", i))
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
		wb.Prewrite(expectKey, &expectLock)
		apply.execWriteCmd(applyCtx, &rfpb.RaftCmdRequest{
			Header:   new(rfpb.RaftRequestHeader),
			Requests: wb.requests,
		})
		applyCtx.wb.WriteToKV(engines.kv)
		applyCtx.wb.Reset()
		wb.requests = nil
		val := engines.kv.lockStore.Get(expectKey, nil)
		assert.NotNil(t, val)
		lock := mvcc.DecodeLock(val)
		assert.Equal(t, expectLock, lock)
	}

	// Testing Commit
	wb = &raftWriteBatch{
		startTS:  100,
		commitTS: 200,
	}
	for i := 0; i < 3; i++ {
		expectKey := []byte(fmt.Sprintf("%08d", i))
		expectLock := &mvcc.MvccLock{
			MvccLockHdr: mvcc.MvccLockHdr{
				StartTS: 100,
				TTL:     10,
				Op:      uint8(mvcc.LockTypePut),
			},
			Value: values[i],
		}
		wb.Commit(expectKey, expectLock)
		apply.execWriteCmd(applyCtx, &rfpb.RaftCmdRequest{
			Header:   new(rfpb.RaftRequestHeader),
			Requests: wb.requests,
		})
		applyCtx.wb.WriteToKV(engines.kv)
		applyCtx.wb.Reset()
		wb.requests = nil
		engines.kv.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(expectKey)
			assert.Nil(t, err)
			assert.NotNil(t, item)
			userMeta := mvcc.DBUserMeta(item.UserMeta())
			assert.Equal(t, userMeta.StartTS(), expectLock.StartTS)
			assert.Equal(t, userMeta.CommitTS(), wb.commitTS)
			return nil
		})
	}
}

func TestRaftWriteBatch_Rollback(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)
	apply := new(applyDelegate)
	applyCtx := newApplyContext("test", nil, engines, nil, notifier{}, NewDefaultConfig())
	dBWriter := new(raftDBWriter)
	wb := (dBWriter.NewWriteBatch(100, 0)).(*raftWriteBatch)

	values := [][]byte{
		[]byte("large value .........."),
		[]byte("large value .........."),
	}

	for i := 0; i < 2; i++ {
		expectKey := []byte(fmt.Sprintf("%08d", i))
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
		wb.Prewrite(expectKey, &expectLock)
		apply.execWriteCmd(applyCtx, &rfpb.RaftCmdRequest{
			Header:   new(rfpb.RaftRequestHeader),
			Requests: wb.requests,
		})
		applyCtx.wb.WriteToKV(engines.kv)
		applyCtx.wb.Reset()
		wb.requests = nil
	}

	// Testing RollBack
	wb = &raftWriteBatch{
		startTS:  150,
		commitTS: 200,
	}
	expectKey := []byte(fmt.Sprintf("%08d", 0))
	wb.Rollback(expectKey, false)
	apply.execWriteCmd(applyCtx, &rfpb.RaftCmdRequest{
		Header:   new(rfpb.RaftRequestHeader),
		Requests: wb.requests,
	})
	applyCtx.wb.WriteToKV(engines.kv)
	applyCtx.wb.Reset()

	wb = &raftWriteBatch{
		startTS:  100,
		commitTS: 200,
	}
	expectKey = []byte(fmt.Sprintf("%08d", 1))
	wb.Rollback(expectKey, true)
	apply.execWriteCmd(applyCtx, &rfpb.RaftCmdRequest{
		Header:   new(rfpb.RaftRequestHeader),
		Requests: wb.requests,
	})
	applyCtx.wb.WriteToKV(engines.kv)
	applyCtx.wb.Reset()
	// The lock should be deleted.
	val := engines.kv.lockStore.Get(expectKey, nil)
	assert.Nil(t, val)
}
