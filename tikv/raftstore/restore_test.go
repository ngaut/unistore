package raftstore

import (
	"testing"

	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rcpb "github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/stretchr/testify/require"
)

func genEntry(wb *raftWriteBatch, t *testing.T) *eraftpb.Entry {
	header := &rcpb.RaftRequestHeader{
		RegionId: 1,
		Term:     1,
	}
	request := &rcpb.RaftCmdRequest{
		Header:   header,
		Requests: wb.requests,
	}
	data, err := request.Marshal()
	require.Nil(t, err)
	entry := &eraftpb.Entry{Data: data, Context: nil}
	return entry
}

func TestRestore(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)
	wb := &raftWriteBatch{
		startTS:  1,
		commitTS: 0,
	}
	k1 := []byte("tk")
	v1 := []byte("v")

	lockStore := lockstore.NewMemStore(1000)
	rollbackStore := lockstore.NewMemStore(1000)
	// Restore prewrite
	expectLock := mvcc.MvccLock{
		MvccLockHdr: mvcc.MvccLockHdr{
			StartTS:    1,
			TTL:        10,
			Op:         uint8(kvrpcpb.Op_Put),
			PrimaryLen: uint16(len(k1)),
		},
		Primary: k1,
		Value:   v1,
	}
	wb.Prewrite(k1, &expectLock, false)
	txn := engines.kv.DB.NewTransaction(true)
	err := restoreAppliedEntry(genEntry(wb, t), txn, lockStore, rollbackStore)
	require.Nil(t, err)

	// Restore commit
	wbCommit := &raftWriteBatch{
		startTS:  1,
		commitTS: 2,
	}
	wbCommit.Commit(k1, &expectLock)
	txn = engines.kv.DB.NewTransaction(true)
	err = restoreAppliedEntry(genEntry(wbCommit, t), txn, lockStore, rollbackStore)
	require.Nil(t, err)

	// Restore common rollback
	wbRollback := &raftWriteBatch{
		startTS:  3,
		commitTS: 0,
	}
	wbRollback.Rollback(k1, true)
	txn = engines.kv.DB.NewTransaction(true)
	err = restoreAppliedEntry(genEntry(wbRollback, t), txn, lockStore, rollbackStore)
	require.Nil(t, err)

	// Restore pessimistic rollback
	wbPessimisticRollback := &raftWriteBatch{
		startTS:  5,
		commitTS: 6,
	}
	wbPessimisticRollback.PessimisticRollback(k1)
	txn = engines.kv.DB.NewTransaction(true)
	err = restoreAppliedEntry(genEntry(wbPessimisticRollback, t), txn, lockStore, rollbackStore)
	require.Nil(t, err)
}
