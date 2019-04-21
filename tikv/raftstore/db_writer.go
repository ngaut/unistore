package raftstore

import (
	"fmt"
	"github.com/ngaut/unistore/tikv/mvcc"
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

func (wb *raftWriteBatch) Prewrite(key []byte, lock *mvcc.MvccLock) {
	key = codec.EncodeBytes(nil, key)
	putLock, putDefault, err := mvcc.EncodeLockCFValue(lock)
	if err != nil {
		panic(fmt.Sprintf("Prewrite error when transfering lock. [key: %v, %v]", key, err))
	}
	if len(putDefault) != 0 {
		// Prewrite with large value.
		putDefaultReq := &rfpb.Request{
			CmdType: rfpb.CmdType_Put,
			Put: &rfpb.PutRequest{
				Cf:    "",
				Key:   codec.EncodeUintDesc(key, lock.StartTS),
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
	key = codec.EncodeBytes(nil, key)
	putWriteReq := &rfpb.Request{
		CmdType: rfpb.CmdType_Put,
		Put: &rfpb.PutRequest{
			Cf:    CFWrite,
			Key:   codec.EncodeUintDesc(key, wb.commitTS),
			Value: mvcc.EncodeWriteCFValue(lock.Op, lock.StartTS, lock.Value),
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
	key = codec.EncodeBytes(nil, key)
	rollBackReq := &rfpb.Request{
		CmdType: rfpb.CmdType_Put,
		Put: &rfpb.PutRequest{
			Cf:    CFWrite,
			Key:   codec.EncodeUintDesc(key, wb.startTS),
			Value: mvcc.EncodeWriteCFValue(mvcc.WriteTypeRollback, wb.startTS, nil),
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
