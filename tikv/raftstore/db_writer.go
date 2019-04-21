package raftstore

import (
	"errors"
	"fmt"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rfpb "github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/tidb/util/codec"
)

type raftDBWriter struct {
}

func (writer *raftDBWriter) Open() {
	// TODO: stub
}

func (writer *raftDBWriter) Close() {
	// TODO: stub
}

type raftWriteBatch struct {
	requests []*rfpb.Request
	startTS  uint64
	commitTS uint64
}

// transferLockToValue decode the mvcc lock and returns putLock value and putDefault value if exists.
func transferLockToValue(lock *mvcc.MvccLock) ([]byte, []byte, error) {
	data := make([]byte, 1)
	switch lock.Op {
	case byte(kvrpcpb.Op_Put):
		data[0] = mvcc.LockTypePut
	case byte(kvrpcpb.Op_Del):
		data[0] = mvcc.LockTypeDelete
	case byte(kvrpcpb.Op_Lock):
		data[0] = mvcc.LockTypeLock
	default:
		return nil, nil, errors.New("invalid lock op")
	}
	data = codec.EncodeUvarint(codec.EncodeCompactBytes(data, lock.Primary), lock.StartTS)
	if lock.TTL != 0 {
		data = codec.EncodeUvarint(data, uint64(lock.TTL))
	}
	if len(lock.Value) <= shortValueMaxLen {
		if len(lock.Value) != 0 {
			data = append(append(data, byte(len(lock.Value))), lock.Value...)
		}
		return data, nil, nil
	} else {
		return data, lock.Value, nil
	}
}

func (wb *raftWriteBatch) Prewrite(key []byte, lock *mvcc.MvccLock) {
	putLock, putDefault, err := transferLockToValue(lock)
	if err != nil {
		panic(fmt.Sprintf("Prewrite error when transfering lock. [key: %v, %v]", key, err))
	}
	if len(putDefault) != 0 {
		// Prewrite with large value.
		putDefaultReq := &rfpb.Request{
			CmdType: rfpb.CmdType_Put,
			Put: &rfpb.PutRequest{
				Cf:    "",
				Key:   codec.EncodeBytes(nil, codec.EncodeUintDesc(key, lock.StartTS)),
				Value: putDefault,
			},
		}
		putLockReq := &rfpb.Request{
			CmdType: rfpb.CmdType_Put,
			Put: &rfpb.PutRequest{
				Cf:    CFLock,
				Key:   key,
				Value: putLock,
			},
		}
		wb.requests = append(wb.requests, putDefaultReq, putLockReq)
	} else {
		putLockReq := &rfpb.Request{
			CmdType: rfpb.CmdType_Put,
			Put: &rfpb.PutRequest{
				Cf:    CFLock,
				Key:   key,
				Value: putLock,
			},
		}
		wb.requests = append(wb.requests, putLockReq)
	}
}

func (wb *raftWriteBatch) Commit(key []byte, lock *mvcc.MvccLock) {
	putWriteReq := &rfpb.Request{
		CmdType: rfpb.CmdType_Put,
		Put: &rfpb.PutRequest{
			Cf:    CFWrite,
			Key:   codec.EncodeBytes(nil, codec.EncodeUintDesc(key, wb.commitTS)),
			Value: mvcc.TransferWriteCfToBytes(lock.Op, lock.StartTS, lock.Value),
		},
	}
	delLockReq := &rfpb.Request{
		CmdType: rfpb.CmdType_Delete,
		Delete: &rfpb.DeleteRequest{
			Cf:  CFLock,
			Key: key,
		},
	}
	wb.requests = append(wb.requests, putWriteReq, delLockReq)
}

func (wb *raftWriteBatch) Rollback(key []byte, deleteLock bool) {
	rollBackReq := &rfpb.Request{
		CmdType: rfpb.CmdType_Put,
		Put: &rfpb.PutRequest{
			Cf:    CFWrite,
			Key:   codec.EncodeBytes(nil, codec.EncodeUintDesc(key, wb.startTS)),
			Value: mvcc.TransferWriteCfToBytes(mvcc.WriteTypeRollback, wb.startTS, nil),
		},
	}
	if deleteLock {
		delLockReq := &rfpb.Request{
			CmdType: rfpb.CmdType_Delete,
			Delete: &rfpb.DeleteRequest{
				Cf:  CFLock,
				Key: key,
			},
		}
		wb.requests = append(wb.requests, rollBackReq, delLockReq)
	} else {
		wb.requests = append(wb.requests, rollBackReq)
	}
}

func (writer *raftDBWriter) NewWriteBatch(startTS, commitTS uint64) mvcc.WriteBatch {
	return &raftWriteBatch{
		startTS:  startTS,
		commitTS: commitTS,
	}
}

func (writer *raftDBWriter) Write(batch mvcc.WriteBatch) error {
	return nil // TODO
}

func (writer *raftDBWriter) DeleteRange(startKey, endKey []byte, latchHandle mvcc.LatchHandle) error {
	return nil // TODO: stub
}

func NewDBWriter(bundle *mvcc.DBBundle, config *Config) mvcc.DBWriter {
	// TODO: stub
	return &raftDBWriter{}
}
