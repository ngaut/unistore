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
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/ngaut/unistore/tikv/raftstore"
	"github.com/ngaut/unistore/util/lockwaiter"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	deadlockPb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/mockstore/unistore/cophandler"
	"go.uber.org/zap"
)

var _ tikvpb.TikvServer = new(Server)

type Server struct {
	mvccStore     *MVCCStore
	regionManager RegionManager
	innerServer   InnerServer
	wg            sync.WaitGroup
	refCount      int32
	stopped       int32
}

func NewServer(rm RegionManager, store *MVCCStore, innerServer InnerServer) *Server {
	return &Server{
		mvccStore:     store,
		regionManager: rm,
		innerServer:   innerServer,
	}
}

const requestMaxSize = 6 * 1024 * 1024

func (svr *Server) checkRequestSize(size int) *errorpb.Error {
	// TiKV has a limitation on raft log size.
	// mocktikv has no raft inside, so we check the request's size instead.
	if size >= requestMaxSize {
		return &errorpb.Error{
			RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{},
		}
	}
	return nil
}

func (svr *Server) Stop() {
	atomic.StoreInt32(&svr.stopped, 1)
	for {
		if atomic.LoadInt32(&svr.refCount) == 0 {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	if err := svr.mvccStore.Close(); err != nil {
		log.Error("close mvcc store failed", zap.Error(err))
	}
	if err := svr.regionManager.Close(); err != nil {
		log.Error("close region manager failed", zap.Error(err))
	}
	if err := svr.innerServer.Stop(); err != nil {
		log.Error("close inner server failed", zap.Error(err))
	}
}

type requestCtx struct {
	svr              *Server
	regCtx           *regionCtx
	regErr           *errorpb.Error
	buf              []byte
	reader           *dbreader.DBReader
	method           string
	startTime        time.Time
	rpcCtx           *kvrpcpb.Context
	asyncMinCommitTS uint64
}

func newRequestCtx(svr *Server, ctx *kvrpcpb.Context, method string) (*requestCtx, error) {
	atomic.AddInt32(&svr.refCount, 1)
	if atomic.LoadInt32(&svr.stopped) > 0 {
		atomic.AddInt32(&svr.refCount, -1)
		return nil, ErrRetryable("server is closed")
	}
	req := &requestCtx{
		svr:       svr,
		method:    method,
		startTime: time.Now(),
		rpcCtx:    ctx,
	}
	req.regCtx, req.regErr = svr.regionManager.GetRegionFromCtx(ctx)
	return req, nil
}

// For read-only requests that doesn't acquire latches, this function must be called after all locks has been checked.
func (req *requestCtx) getDBReader() *dbreader.DBReader {
	if req.reader == nil {
		mvccStore := req.svr.mvccStore
		txn := mvccStore.db.NewTransaction(false)
		req.reader = dbreader.NewDBReader(req.regCtx.startKey, req.regCtx.endKey, txn)
	}
	return req.reader
}

func (req *requestCtx) finish() {
	atomic.AddInt32(&req.svr.refCount, -1)
	if req.reader != nil {
		req.reader.Close()
	}
}

func (svr *Server) KvGet(ctx context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvGet")
	if err != nil {
		return &kvrpcpb.GetResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.GetResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.CheckKeysLock(req.GetVersion(), req.Context.ResolvedLocks, req.Key)
	if err != nil {
		return &kvrpcpb.GetResponse{Error: convertToKeyError(err)}, nil
	}
	reader := reqCtx.getDBReader()
	val, err := reader.Get(req.Key, req.GetVersion())
	if err != nil {
		return &kvrpcpb.GetResponse{
			Error: convertToKeyError(err),
		}, nil
	}
	val = safeCopy(val)
	return &kvrpcpb.GetResponse{
		Value: val,
	}, nil
}

func (svr *Server) KvScan(ctx context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvScan")
	if err != nil {
		return &kvrpcpb.ScanResponse{Pairs: []*kvrpcpb.KvPair{{Error: convertToKeyError(err)}}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.ScanResponse{RegionError: reqCtx.regErr}, nil
	}
	pairs := svr.mvccStore.Scan(reqCtx, req)
	return &kvrpcpb.ScanResponse{
		Pairs: pairs,
	}, nil
}

func (svr *Server) KvPessimisticLock(ctx context.Context, req *kvrpcpb.PessimisticLockRequest) (*kvrpcpb.PessimisticLockResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "PessimisticLock")
	if err != nil {
		return &kvrpcpb.PessimisticLockResponse{Errors: []*kvrpcpb.KeyError{convertToKeyError(err)}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.PessimisticLockResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := &kvrpcpb.PessimisticLockResponse{}
	waiter, err := svr.mvccStore.PessimisticLock(reqCtx, req, resp)
	resp.Errors, resp.RegionError = convertToPBErrors(err)
	if waiter == nil {
		return resp, nil
	}
	result := waiter.Wait()
	svr.mvccStore.DeadlockDetectCli.CleanUpWaitFor(req.StartVersion, waiter.LockTS, waiter.KeyHash)
	svr.mvccStore.lockWaiterManager.CleanUp(waiter)
	if result.WakeupSleepTime == lockwaiter.WaitTimeout {
		return resp, nil
	}
	if result.DeadlockResp != nil {
		log.Error("deadlock found", zap.Stringer("entry", &result.DeadlockResp.Entry))
		errLocked := err.(*ErrLocked)
		deadlockErr := &ErrDeadlock{
			LockKey:         errLocked.Key,
			LockTS:          errLocked.StartTS,
			DeadlockKeyHash: result.DeadlockResp.DeadlockKeyHash,
		}
		resp.Errors, resp.RegionError = convertToPBErrors(deadlockErr)
		return resp, nil
	}
	if result.WakeupSleepTime == lockwaiter.WakeUpThisWaiter {
		if req.Force {
			req.WaitTimeout = lockwaiter.LockNoWait
			_, err := svr.mvccStore.PessimisticLock(reqCtx, req, resp)
			resp.Errors, resp.RegionError = convertToPBErrors(err)
			if err == nil {
				return resp, nil
			}
			if _, ok := err.(*ErrLocked); !ok {
				resp.Errors, resp.RegionError = convertToPBErrors(err)
				return resp, nil
			}
			log.Warn("wakeup force lock request, try lock still failed", zap.Error(err))
		}
	}
	// The key is rollbacked, we don't have the exact commitTS, but we can use the server's latest.
	// Always use the store latest ts since the waiter result commitTs may not be the real conflict ts
	conflictCommitTS := svr.mvccStore.getLatestTS()
	err = &ErrConflict{
		StartTS:          req.GetForUpdateTs(),
		ConflictTS:       waiter.LockTS,
		ConflictCommitTS: conflictCommitTS,
	}
	resp.Errors, _ = convertToPBErrors(err)
	return resp, nil
}

func (svr *Server) KVPessimisticRollback(ctx context.Context, req *kvrpcpb.PessimisticRollbackRequest) (*kvrpcpb.PessimisticRollbackResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "PessimisticRollback")
	if err != nil {
		return &kvrpcpb.PessimisticRollbackResponse{Errors: []*kvrpcpb.KeyError{convertToKeyError(err)}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.PessimisticRollbackResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.PessimisticRollback(reqCtx, req)
	resp := &kvrpcpb.PessimisticRollbackResponse{}
	resp.Errors, resp.RegionError = convertToPBErrors(err)
	return resp, nil
}

func (svr *Server) KvTxnHeartBeat(ctx context.Context, req *kvrpcpb.TxnHeartBeatRequest) (*kvrpcpb.TxnHeartBeatResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "TxnHeartBeat")
	if err != nil {
		return &kvrpcpb.TxnHeartBeatResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.TxnHeartBeatResponse{RegionError: reqCtx.regErr}, nil
	}
	lockTTL, err := svr.mvccStore.TxnHeartBeat(reqCtx, req)
	resp := &kvrpcpb.TxnHeartBeatResponse{LockTtl: lockTTL}
	resp.Error, resp.RegionError = convertToPBError(err)
	return resp, nil
}

func (svr *Server) KvCheckTxnStatus(ctx context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvCheckTxnStatus")
	if err != nil {
		return &kvrpcpb.CheckTxnStatusResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.CheckTxnStatusResponse{RegionError: reqCtx.regErr}, nil
	}
	txnStatus, err := svr.mvccStore.CheckTxnStatus(reqCtx, req)
	ttl := uint64(0)
	if txnStatus.lockInfo != nil {
		ttl = txnStatus.lockInfo.LockTtl
	}
	resp := &kvrpcpb.CheckTxnStatusResponse{
		LockTtl:       ttl,
		CommitVersion: txnStatus.commitTS,
		Action:        txnStatus.action,
		LockInfo:      txnStatus.lockInfo,
	}
	resp.Error, resp.RegionError = convertToPBError(err)
	return resp, nil
}

func (svr *Server) KvCheckSecondaryLocks(ctx context.Context, req *kvrpcpb.CheckSecondaryLocksRequest) (*kvrpcpb.CheckSecondaryLocksResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvCheckSecondaryLocks")
	if err != nil {
		return &kvrpcpb.CheckSecondaryLocksResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.CheckSecondaryLocksResponse{RegionError: reqCtx.regErr}, nil
	}
	locksStatus, err := svr.mvccStore.CheckSecondaryLocks(reqCtx, req.Keys, req.StartVersion)
	resp := &kvrpcpb.CheckSecondaryLocksResponse{}
	if err == nil {
		resp.Locks = locksStatus.locks
		resp.CommitTs = locksStatus.commitTS
	} else {
		resp.Error, resp.RegionError = convertToPBError(err)
	}
	return resp, nil
}

func (svr *Server) KvPrewrite(ctx context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvPrewrite")
	if err != nil {
		return &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{convertToKeyError(err)}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.PrewriteResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.Prewrite(reqCtx, req)
	resp := &kvrpcpb.PrewriteResponse{}
	if reqCtx.asyncMinCommitTS > 0 {
		resp.MinCommitTs = reqCtx.asyncMinCommitTS
	}
	resp.Errors, resp.RegionError = convertToPBErrors(err)
	return resp, nil
}

func (svr *Server) KvCommit(ctx context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvCommit")
	if err != nil {
		return &kvrpcpb.CommitResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.CommitResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := new(kvrpcpb.CommitResponse)
	err = svr.mvccStore.Commit(reqCtx, req.Keys, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		resp.Error, resp.RegionError = convertToPBError(err)
	}
	return resp, nil
}

func (svr *Server) KvImport(context.Context, *kvrpcpb.ImportRequest) (*kvrpcpb.ImportResponse, error) {
	// TODO
	return &kvrpcpb.ImportResponse{}, nil
}

func (svr *Server) KvCleanup(ctx context.Context, req *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvCleanup")
	if err != nil {
		return &kvrpcpb.CleanupResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.CleanupResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.Cleanup(reqCtx, req.Key, req.StartVersion, req.CurrentTs)
	resp := new(kvrpcpb.CleanupResponse)
	if committed, ok := err.(ErrAlreadyCommitted); ok {
		resp.CommitVersion = uint64(committed)
	} else if err != nil {
		log.Error("cleanup failed", zap.Error(err))
		resp.Error, resp.RegionError = convertToPBError(err)
	}
	return resp, nil
}

func (svr *Server) KvBatchGet(ctx context.Context, req *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvBatchGet")
	if err != nil {
		return &kvrpcpb.BatchGetResponse{Pairs: []*kvrpcpb.KvPair{{Error: convertToKeyError(err)}}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.BatchGetResponse{RegionError: reqCtx.regErr}, nil
	}
	pairs := svr.mvccStore.BatchGet(reqCtx, req.Keys, req.GetVersion())
	return &kvrpcpb.BatchGetResponse{
		Pairs: pairs,
	}, nil
}

func (svr *Server) KvBatchRollback(ctx context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvBatchRollback")
	if err != nil {
		return &kvrpcpb.BatchRollbackResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.BatchRollbackResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := new(kvrpcpb.BatchRollbackResponse)
	err = svr.mvccStore.Rollback(reqCtx, req.Keys, req.StartVersion)
	resp.Error, resp.RegionError = convertToPBError(err)
	return resp, nil
}

func (svr *Server) KvScanLock(ctx context.Context, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvScanLock")
	if err != nil {
		return &kvrpcpb.ScanLockResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.ScanLockResponse{RegionError: reqCtx.regErr}, nil
	}
	log.Debug("kv scan lock")
	locks, err := svr.mvccStore.ScanLock(reqCtx, req.MaxVersion, int(req.Limit))
	return &kvrpcpb.ScanLockResponse{Error: convertToKeyError(err), Locks: locks}, nil
}

func (svr *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvResolveLock")
	if err != nil {
		return &kvrpcpb.ResolveLockResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.ResolveLockResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := &kvrpcpb.ResolveLockResponse{}
	if len(req.TxnInfos) > 0 {
		for _, txnInfo := range req.TxnInfos {
			log.S().Debugf("kv resolve lock region:%d txn:%v", reqCtx.regCtx.meta.Id, txnInfo.Txn)
			err := svr.mvccStore.ResolveLock(reqCtx, nil, txnInfo.Txn, txnInfo.Status)
			if err != nil {
				resp.Error, resp.RegionError = convertToPBError(err)
				break
			}
		}
	} else {
		log.S().Debugf("kv resolve lock region:%d txn:%v", reqCtx.regCtx.meta.Id, req.StartVersion)
		err := svr.mvccStore.ResolveLock(reqCtx, req.Keys, req.StartVersion, req.CommitVersion)
		resp.Error, resp.RegionError = convertToPBError(err)
	}
	return resp, nil
}

func (svr *Server) KvGC(ctx context.Context, req *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvGC")
	if err != nil {
		return &kvrpcpb.GCResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	svr.mvccStore.UpdateSafePoint(req.SafePoint)
	return &kvrpcpb.GCResponse{}, nil
}

func (svr *Server) KvDeleteRange(ctx context.Context, req *kvrpcpb.DeleteRangeRequest) (*kvrpcpb.DeleteRangeResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvDeleteRange")
	if err != nil {
		return &kvrpcpb.DeleteRangeResponse{Error: convertToKeyError(err).String()}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.DeleteRangeResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.dbWriter.DeleteRange(req.StartKey, req.EndKey, reqCtx.regCtx)
	if err != nil {
		log.Error("delete range failed", zap.Error(err))
	}
	return &kvrpcpb.DeleteRangeResponse{}, nil
}

// RawKV commands.
func (svr *Server) RawGet(context.Context, *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	return &kvrpcpb.RawGetResponse{}, nil
}

func (svr *Server) RawPut(context.Context, *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	return &kvrpcpb.RawPutResponse{}, nil
}

func (svr *Server) RawDelete(context.Context, *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	return &kvrpcpb.RawDeleteResponse{}, nil
}

func (svr *Server) RawScan(context.Context, *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	return &kvrpcpb.RawScanResponse{}, nil
}

func (svr *Server) RawBatchDelete(context.Context, *kvrpcpb.RawBatchDeleteRequest) (*kvrpcpb.RawBatchDeleteResponse, error) {
	return &kvrpcpb.RawBatchDeleteResponse{}, nil
}

func (svr *Server) RawBatchGet(context.Context, *kvrpcpb.RawBatchGetRequest) (*kvrpcpb.RawBatchGetResponse, error) {
	return &kvrpcpb.RawBatchGetResponse{}, nil
}

func (svr *Server) RawBatchPut(context.Context, *kvrpcpb.RawBatchPutRequest) (*kvrpcpb.RawBatchPutResponse, error) {
	return &kvrpcpb.RawBatchPutResponse{}, nil
}

func (svr *Server) RawBatchScan(context.Context, *kvrpcpb.RawBatchScanRequest) (*kvrpcpb.RawBatchScanResponse, error) {
	return &kvrpcpb.RawBatchScanResponse{}, nil
}

func (svr *Server) RawDeleteRange(context.Context, *kvrpcpb.RawDeleteRangeRequest) (*kvrpcpb.RawDeleteRangeResponse, error) {
	return &kvrpcpb.RawDeleteRangeResponse{}, nil
}

// SQL push down commands.
func (svr *Server) Coprocessor(ctx context.Context, req *coprocessor.Request) (*coprocessor.Response, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "Coprocessor")
	if err != nil {
		return &coprocessor.Response{OtherError: convertToKeyError(err).String()}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &coprocessor.Response{RegionError: reqCtx.regErr}, nil
	}
	return cophandler.HandleCopRequest(reqCtx.getDBReader(), svr.mvccStore.lockStore, req), nil
}

func (svr *Server) CoprocessorStream(*coprocessor.Request, tikvpb.Tikv_CoprocessorStreamServer) error {
	// TODO
	return nil
}

func (svr *Server) BatchCoprocessor(*coprocessor.BatchRequest, tikvpb.Tikv_BatchCoprocessorServer) error {
	panic("todo")
}

// Raft commands (tikv <-> tikv).
func (svr *Server) Raft(stream tikvpb.Tikv_RaftServer) error {
	return svr.innerServer.Raft(stream)
}
func (svr *Server) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return svr.innerServer.Snapshot(stream)
}

func (svr *Server) BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error {
	return svr.innerServer.BatchRaft(stream)
}

// Region commands.
func (svr *Server) SplitRegion(ctx context.Context, req *kvrpcpb.SplitRegionRequest) (*kvrpcpb.SplitRegionResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "SplitRegion")
	if err != nil {
		return &kvrpcpb.SplitRegionResponse{RegionError: &errorpb.Error{Message: err.Error()}}, nil
	}
	defer reqCtx.finish()
	return svr.regionManager.SplitRegion(req), nil
}

func (svr *Server) ReadIndex(context.Context, *kvrpcpb.ReadIndexRequest) (*kvrpcpb.ReadIndexResponse, error) {
	// TODO:
	return &kvrpcpb.ReadIndexResponse{}, nil
}

// transaction debugger commands.
func (svr *Server) MvccGetByKey(ctx context.Context, req *kvrpcpb.MvccGetByKeyRequest) (*kvrpcpb.MvccGetByKeyResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "MvccGetByKey")
	if err != nil {
		return &kvrpcpb.MvccGetByKeyResponse{Error: err.Error()}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.MvccGetByKeyResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := new(kvrpcpb.MvccGetByKeyResponse)
	mvccInfo, err := svr.mvccStore.MvccGetByKey(reqCtx, req.GetKey())
	if err != nil {
		resp.Error = err.Error()
	}
	resp.Info = mvccInfo
	return resp, nil
}

func (svr *Server) MvccGetByStartTs(ctx context.Context, req *kvrpcpb.MvccGetByStartTsRequest) (*kvrpcpb.MvccGetByStartTsResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "MvccGetByStartTs")
	if err != nil {
		return &kvrpcpb.MvccGetByStartTsResponse{Error: err.Error()}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.MvccGetByStartTsResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := new(kvrpcpb.MvccGetByStartTsResponse)
	mvccInfo, key, err := svr.mvccStore.MvccGetByStartTs(reqCtx, req.StartTs)
	if err != nil {
		resp.Error = err.Error()
	}
	resp.Info = mvccInfo
	resp.Key = key
	return resp, nil
}

func (svr *Server) UnsafeDestroyRange(ctx context.Context, req *kvrpcpb.UnsafeDestroyRangeRequest) (*kvrpcpb.UnsafeDestroyRangeResponse, error) {
	start, end := req.GetStartKey(), req.GetEndKey()
	svr.mvccStore.DeleteFileInRange(start, end)
	return &kvrpcpb.UnsafeDestroyRangeResponse{}, nil
}

// deadlock detection related services
// GetWaitForEntries tries to get the waitFor entries
func (svr *Server) GetWaitForEntries(ctx context.Context,
	req *deadlockPb.WaitForEntriesRequest) (*deadlockPb.WaitForEntriesResponse, error) {
	// TODO
	return &deadlockPb.WaitForEntriesResponse{}, nil
}

// Detect will handle detection rpc from other nodes
func (svr *Server) Detect(stream deadlockPb.Deadlock_DetectServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if !svr.mvccStore.DeadlockDetectSvr.IsLeader() {
			log.Warn("detection requests received on non leader node")
			break
		}
		resp := svr.mvccStore.DeadlockDetectSvr.Detect(req)
		if resp != nil {
			if sendErr := stream.Send(resp); sendErr != nil {
				log.Error("send deadlock response failed", zap.Error(sendErr))
				break
			}
		}
	}
	return nil
}

func (svr *Server) CheckLockObserver(context.Context, *kvrpcpb.CheckLockObserverRequest) (*kvrpcpb.CheckLockObserverResponse, error) {
	// TODO: implement Observer
	return &kvrpcpb.CheckLockObserverResponse{IsClean: true}, nil
}

func (svr *Server) PhysicalScanLock(ctx context.Context, req *kvrpcpb.PhysicalScanLockRequest) (*kvrpcpb.PhysicalScanLockResponse, error) {
	resp := &kvrpcpb.PhysicalScanLockResponse{}
	resp.Locks = svr.mvccStore.PhysicalScanLock(req.StartKey, req.MaxTs, int(req.Limit))
	return resp, nil
}

func (svr *Server) RegisterLockObserver(context.Context, *kvrpcpb.RegisterLockObserverRequest) (*kvrpcpb.RegisterLockObserverResponse, error) {
	// TODO: implement Observer
	return &kvrpcpb.RegisterLockObserverResponse{}, nil
}

func (svr *Server) RemoveLockObserver(context.Context, *kvrpcpb.RemoveLockObserverRequest) (*kvrpcpb.RemoveLockObserverResponse, error) {
	// TODO: implement Observer
	return &kvrpcpb.RemoveLockObserverResponse{}, nil
}

func (svr *Server) VerGet(context.Context, *kvrpcpb.VerGetRequest) (*kvrpcpb.VerGetResponse, error) {
	panic("unimplemented")
}

func (svr *Server) VerBatchGet(context.Context, *kvrpcpb.VerBatchGetRequest) (*kvrpcpb.VerBatchGetResponse, error) {
	panic("unimplemented")
}

func (svr *Server) VerMut(context.Context, *kvrpcpb.VerMutRequest) (*kvrpcpb.VerMutResponse, error) {
	panic("unimplemented")
}

func (svr *Server) VerBatchMut(context.Context, *kvrpcpb.VerBatchMutRequest) (*kvrpcpb.VerBatchMutResponse, error) {
	panic("unimplemented")
}

func (svr *Server) VerScan(context.Context, *kvrpcpb.VerScanRequest) (*kvrpcpb.VerScanResponse, error) {
	panic("unimplemented")
}

func (svr *Server) VerDeleteRange(context.Context, *kvrpcpb.VerDeleteRangeRequest) (*kvrpcpb.VerDeleteRangeResponse, error) {
	panic("unimplemented")
}

func convertToKeyError(err error) *kvrpcpb.KeyError {
	if err == nil {
		return nil
	}
	causeErr := errors.Cause(err)
	switch x := causeErr.(type) {
	case *ErrLocked:
		return &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: x.Primary,
				Key:         x.Key,
				LockVersion: x.StartTS,
				LockTtl:     x.TTL,
				LockType:    kvrpcpb.Op(x.LockType),
				MinCommitTs: x.minCommitTS,
			},
		}
	case ErrRetryable:
		return &kvrpcpb.KeyError{
			Retryable: x.Error(),
		}
	case *ErrKeyAlreadyExists:
		return &kvrpcpb.KeyError{
			AlreadyExist: &kvrpcpb.AlreadyExist{
				Key: x.Key,
			},
		}
	case *ErrConflict:
		return &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:          x.StartTS,
				ConflictTs:       x.ConflictTS,
				ConflictCommitTs: x.ConflictCommitTS,
				Key:              x.Key,
			},
		}
	case *ErrDeadlock:
		return &kvrpcpb.KeyError{
			Deadlock: &kvrpcpb.Deadlock{
				LockKey:         x.LockKey,
				LockTs:          x.LockTS,
				DeadlockKeyHash: x.DeadlockKeyHash,
			},
		}
	case *ErrCommitExpire:
		return &kvrpcpb.KeyError{
			CommitTsExpired: &kvrpcpb.CommitTsExpired{
				StartTs:           x.StartTs,
				AttemptedCommitTs: x.CommitTs,
				Key:               x.Key,
				MinCommitTs:       x.MinCommitTs,
			},
		}
	case *ErrTxnNotFound:
		return &kvrpcpb.KeyError{
			TxnNotFound: &kvrpcpb.TxnNotFound{
				StartTs:    x.StartTS,
				PrimaryKey: x.PrimaryKey,
			},
		}
	default:
		return &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
	}
}

func convertToPBError(err error) (*kvrpcpb.KeyError, *errorpb.Error) {
	if regErr := extractRegionError(err); regErr != nil {
		return nil, regErr
	}
	return convertToKeyError(err), nil
}

func convertToPBErrors(err error) ([]*kvrpcpb.KeyError, *errorpb.Error) {
	if err != nil {
		if regErr := extractRegionError(err); regErr != nil {
			return nil, regErr
		}
		return []*kvrpcpb.KeyError{convertToKeyError(err)}, nil
	}
	return nil, nil
}

func extractRegionError(err error) *errorpb.Error {
	if raftError, ok := err.(*raftstore.RaftError); ok {
		return raftError.RequestErr
	}
	return nil
}
