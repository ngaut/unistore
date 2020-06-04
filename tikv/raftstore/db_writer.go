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
	"time"

	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/metrics"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/ngaut/unistore/tikv/raftstore/raftlog"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rcpb "github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/tidb/util/codec"
)

type raftDBWriter struct {
	router           *router
	useCustomRaftLog bool
}

func (writer *raftDBWriter) Open() {
	// TODO: stub
}

func (writer *raftDBWriter) Close() {
	// TODO: stub
}

type raftWriteBatch struct {
	ctx      *kvrpcpb.Context
	requests []*rcpb.Request
	startTS  uint64
	commitTS uint64
}

func (wb *raftWriteBatch) Prewrite(key []byte, lock *mvcc.MvccLock) {
	encodedKey := codec.EncodeBytes(nil, key)
	putLock, putDefault := mvcc.EncodeLockCFValue(lock)
	if len(putDefault) != 0 {
		// Prewrite with large value.
		putDefaultReq := &rcpb.Request{
			CmdType: rcpb.CmdType_Put,
			Put: &rcpb.PutRequest{
				Cf:    "",
				Key:   codec.EncodeUintDesc(encodedKey, lock.StartTS),
				Value: putDefault,
			},
		}
		putLockReq := &rcpb.Request{
			CmdType: rcpb.CmdType_Put,
			Put: &rcpb.PutRequest{
				Cf:    CFLock,
				Key:   encodedKey,
				Value: putLock,
			},
		}
		wb.requests = append(wb.requests, putDefaultReq, putLockReq)
	} else {
		putLockReq := &rcpb.Request{
			CmdType: rcpb.CmdType_Put,
			Put: &rcpb.PutRequest{
				Cf:    CFLock,
				Key:   encodedKey,
				Value: putLock,
			},
		}
		wb.requests = append(wb.requests, putLockReq)
	}
}

func (wb *raftWriteBatch) Commit(key []byte, lock *mvcc.MvccLock) {
	encodedKey := codec.EncodeBytes(nil, key)
	writeType := mvcc.WriteTypePut
	switch lock.Op {
	case byte(kvrpcpb.Op_Lock):
		writeType = mvcc.WriteTypeLock
	case byte(kvrpcpb.Op_Del):
		writeType = mvcc.WriteTypeDelete
	}
	putWriteReq := &rcpb.Request{
		CmdType: rcpb.CmdType_Put,
		Put: &rcpb.PutRequest{
			Cf:    CFWrite,
			Key:   codec.EncodeUintDesc(encodedKey, wb.commitTS),
			Value: mvcc.EncodeWriteCFValue(writeType, lock.StartTS, lock.Value),
		},
	}
	delLockReq := &rcpb.Request{
		CmdType: rcpb.CmdType_Delete,
		Delete: &rcpb.DeleteRequest{
			Cf:  CFLock,
			Key: encodedKey,
		},
	}
	wb.requests = append(wb.requests, putWriteReq, delLockReq)
}

func (wb *raftWriteBatch) Rollback(key []byte, deleteLock bool) {
	encodedKey := codec.EncodeBytes(nil, key)
	rollBackReq := &rcpb.Request{
		CmdType: rcpb.CmdType_Put,
		Put: &rcpb.PutRequest{
			Cf:    CFWrite,
			Key:   codec.EncodeUintDesc(encodedKey, wb.startTS),
			Value: mvcc.EncodeWriteCFValue(mvcc.WriteTypeRollback, wb.startTS, nil),
		},
	}
	if deleteLock {
		delLockReq := &rcpb.Request{
			CmdType: rcpb.CmdType_Delete,
			Delete: &rcpb.DeleteRequest{
				Cf:  CFLock,
				Key: encodedKey,
			},
		}
		wb.requests = append(wb.requests, rollBackReq, delLockReq)
	} else {
		wb.requests = append(wb.requests, rollBackReq)
	}
}

func (wb *raftWriteBatch) PessimisticLock(key []byte, lock *mvcc.MvccLock) {
	encodedKey := codec.EncodeBytes(nil, key)
	val, _ := mvcc.EncodeLockCFValue(lock)
	wb.requests = append(wb.requests, &rcpb.Request{
		CmdType: rcpb.CmdType_Put,
		Put: &rcpb.PutRequest{
			Cf:    CFLock,
			Key:   encodedKey,
			Value: val,
		},
	})
}

func (wb *raftWriteBatch) PessimisticRollback(key []byte) {
	encodedKey := codec.EncodeBytes(nil, key)
	wb.requests = append(wb.requests, &rcpb.Request{
		CmdType: rcpb.CmdType_Delete,
		Delete: &rcpb.DeleteRequest{
			Cf:  CFLock,
			Key: encodedKey,
		},
	})
}

func (writer *raftDBWriter) NewWriteBatch(startTS, commitTS uint64, ctx *kvrpcpb.Context) mvcc.WriteBatch {
	if writer.useCustomRaftLog {
		return NewCustomWriteBatch(startTS, commitTS, ctx)
	}
	return &raftWriteBatch{
		ctx:      ctx,
		startTS:  startTS,
		commitTS: commitTS,
	}
}

func (writer *raftDBWriter) Write(batch mvcc.WriteBatch) error {
	cmd := &MsgRaftCmd{
		SendTime: time.Now(),
		Callback: NewCallback(),
	}
	var reqLen int
	switch x := batch.(type) {
	case *raftWriteBatch:
		ctx := x.ctx
		header := &rcpb.RaftRequestHeader{
			RegionId:    ctx.RegionId,
			Peer:        ctx.Peer,
			RegionEpoch: ctx.RegionEpoch,
			Term:        ctx.Term,
		}
		cmd.Request = raftlog.NewRequest(&rcpb.RaftCmdRequest{
			Header:   header,
			Requests: x.requests,
		})
		reqLen = len(x.requests)
	case *customWriteBatch:
		cmd.Request = x.builder.Build()
		reqLen = x.builder.Len()
	}
	start := time.Now()
	err := writer.router.sendRaftCommand(cmd)
	if err != nil {
		return err
	}
	cmd.Callback.wg.Wait()
	waitDoneTime := time.Now()
	metrics.RaftWriterWait.Observe(waitDoneTime.Sub(start).Seconds())
	cb := cmd.Callback
	if !cb.raftBeginTime.IsZero() {
		metrics.WriteWaiteStepOne.Observe(cb.raftBeginTime.Sub(start).Seconds())
		metrics.WriteWaiteStepTwo.Observe(cb.raftDoneTime.Sub(cb.raftBeginTime).Seconds())
		metrics.WriteWaiteStepThree.Observe(cb.applyBeginTime.Sub(cb.raftDoneTime).Seconds())
		metrics.WriteWaiteStepFour.Observe(cb.applyDoneTime.Sub(cb.applyBeginTime).Seconds())
	}
	return writer.checkResponse(cb.resp, reqLen)
}

type RaftError struct {
	RequestErr *errorpb.Error
}

func (re *RaftError) Error() string {
	return re.RequestErr.String()
}

func (writer *raftDBWriter) checkResponse(resp *rcpb.RaftCmdResponse, reqCount int) error {
	if resp.Header.Error != nil {
		return &RaftError{RequestErr: resp.Header.Error}
	}
	if len(resp.Responses) != reqCount {
		return errors.Errorf("responses count %d is not equal to requests count %d",
			len(resp.Responses), reqCount)
	}
	return nil
}

func (writer *raftDBWriter) DeleteRange(startKey, endKey []byte, latchHandle mvcc.LatchHandle) error {
	return nil // TODO: stub
}

func NewDBWriter(conf *config.Config, router *RaftstoreRouter) mvcc.DBWriter {
	return &raftDBWriter{
		router:           router.router,
		useCustomRaftLog: conf.RaftStore.CustomRaftLog,
	}
}

// TestRaftWriter is used to mock raft write related prewrite and commit operations without
// sending real raft commands
type TestRaftWriter struct {
	dbBundle *mvcc.DBBundle
	engine   *Engines
}

func (w *TestRaftWriter) Open() {
}

func (w *TestRaftWriter) Close() {
}

func (w *TestRaftWriter) Write(batch mvcc.WriteBatch) error {
	raftWriteBatch := batch.(*customWriteBatch)
	raftLog := raftWriteBatch.builder.Build()
	applier := new(applier)
	applyCtx := newApplyContext("test", nil, w.engine, nil, NewDefaultConfig())
	applier.execWriteCmd(applyCtx, raftLog)
	err := applyCtx.wb.WriteToKV(w.dbBundle)
	if err != nil {
		return err
	}
	return nil
}

func (w *TestRaftWriter) DeleteRange(start, end []byte, latchHandle mvcc.LatchHandle) error {
	return nil
}

func (w *TestRaftWriter) NewWriteBatch(startTS, commitTS uint64, ctx *kvrpcpb.Context) mvcc.WriteBatch {
	return NewCustomWriteBatch(startTS, commitTS, ctx)
}

func NewTestRaftWriter(dbBundle *mvcc.DBBundle, engine *Engines) mvcc.DBWriter {
	writer := &TestRaftWriter{
		dbBundle: dbBundle,
		engine:   engine,
	}
	return writer
}

type customWriteBatch struct {
	startTS  uint64
	commitTS uint64
	builder  *raftlog.CustomBuilder
}

func (wb *customWriteBatch) setType(tp raftlog.CustomRaftLogType) {
	oldTp := wb.builder.GetType()
	if oldTp == 0 {
		wb.builder.SetType(tp)
	} else {
		y.Assert(tp == oldTp)
	}
}

func (wb *customWriteBatch) Prewrite(key []byte, lock *mvcc.MvccLock) {
	wb.setType(raftlog.TypePrewrite)
	wb.builder.AppendLock(key, lock.MarshalBinary())
}

func (wb *customWriteBatch) Commit(key []byte, lock *mvcc.MvccLock) {
	wb.setType(raftlog.TypeCommit)
	wb.builder.AppendCommit(key, lock.MarshalBinary(), wb.commitTS)
}

func (wb *customWriteBatch) Rollback(key []byte, deleleLock bool) {
	wb.setType(raftlog.TypeRolback)
	wb.builder.AppendRollback(key, wb.startTS, deleleLock)
}

func (wb *customWriteBatch) PessimisticLock(key []byte, lock *mvcc.MvccLock) {
	wb.setType(raftlog.TypePessimisticLock)
	wb.builder.AppendLock(key, lock.MarshalBinary())
}

func (wb *customWriteBatch) PessimisticRollback(key []byte) {
	wb.setType(raftlog.TypePessimisticRollback)
	wb.builder.AppendPessimisticRollback(key)
}

func NewCustomWriteBatch(startTS, commitTS uint64, ctx *kvrpcpb.Context) mvcc.WriteBatch {
	header := raftlog.CustomHeader{
		RegionID: ctx.RegionId,
		Epoch:    raftlog.NewEpoch(ctx.RegionEpoch.Version, ctx.RegionEpoch.ConfVer),
		PeerID:   ctx.Peer.Id,
		StoreID:  ctx.Peer.StoreId,
		Term:     ctx.Term,
	}
	b := raftlog.NewBuilder(header)
	return &customWriteBatch{
		startTS:  startTS,
		commitTS: commitTS,
		builder:  b,
	}
}
