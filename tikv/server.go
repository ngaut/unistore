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
	"context"
	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/engine"
	"github.com/ngaut/unistore/engine/dfs"
	"github.com/ngaut/unistore/engine/dfs/builtindfs"
	"github.com/ngaut/unistore/engine/dfs/s3dfs"
	"github.com/ngaut/unistore/metrics"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/raftengine"
	"github.com/ngaut/unistore/tikv/cophandler"
	"github.com/ngaut/unistore/tikv/enginereader"
	"github.com/ngaut/unistore/tikv/lockwaiter"
	"github.com/ngaut/unistore/tikv/raftstore"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unicode"
)

var _ tikvpb.TikvServer = new(Server)

type Server struct {
	mvccStore     *MVCCStore
	regionManager RegionManager
	innerServer   *raftstore.InnerServer
	wg            sync.WaitGroup
	stopped       int32
}

const (
	subPathRaft = "raft"
	subPathKV   = "kv"
)

func NewServer(conf *config.Config, pdClient pd.Client) (*Server, error) {
	path := conf.Engine.Path
	kvPath := filepath.Join(path, "kv")
	raftPath := filepath.Join(path, "raft")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)

	raftConf := raftstore.NewDefaultConfig()
	setupRaftStoreConf(raftConf, conf)

	listener := raftstore.NewMetaChangeListener()
	allocator := &idAllocator{
		pdCli: pdClient,
	}
	raftEngine, err := createRaftEngine(subPathRaft, &conf.RaftEngine)
	if err != nil {
		return nil, errors.AddStack(err)
	}
	recoverHandler, err := raftstore.NewRecoverHandler(raftEngine)
	if err != nil {
		return nil, errors.AddStack(err)
	}
	eng, err := createKVEngine(subPathKV, listener, allocator, recoverHandler, pdClient, &conf.Engine, conf.Server.StoreAddr)
	if err != nil {
		return nil, errors.AddStack(err)
	}
	http.DefaultServeMux.HandleFunc("/debug/db", eng.DebugHandler())
	engines := raftstore.NewEngines(eng, raftEngine, kvPath, raftPath, listener)
	innerServer := raftstore.NewRaftInnerServer(conf, engines, raftConf)
	innerServer.Setup(pdClient)
	router := innerServer.GetRaftstoreRouter()
	storeMeta := innerServer.GetStoreMeta()
	store := NewMVCCStore(conf, eng, path, raftstore.NewEngineWriter(router), pdClient)
	rm := NewRaftRegionManager(storeMeta, router, store.DeadlockDetectSvr)
	innerServer.SetPeerEventObserver(rm)

	if err := innerServer.Start(pdClient); err != nil {
		return nil, errors.AddStack(err)
	}

	store.StartDeadlockDetection(true)
	return &Server{
		mvccStore:     store,
		regionManager: rm,
		innerServer:   innerServer,
	}, nil
}

type idAllocator struct {
	pdCli pd.Client
}

func (a *idAllocator) AllocID(count int) (uint64, error) {
	lastTS, tsErr := a.pdCli.GetTS(context.Background(), count)
	if tsErr != nil {
		return 0, tsErr
	}
	return lastTS, nil
}

func setupRaftStoreConf(raftConf *raftstore.Config, conf *config.Config) {
	raftConf.Addr = conf.Server.StoreAddr

	// raftstore block
	raftConf.PdHeartbeatTickInterval = config.ParseDuration(conf.RaftStore.PdHeartbeatTickInterval)
	raftConf.RaftStoreMaxLeaderLease = config.ParseDuration(conf.RaftStore.RaftStoreMaxLeaderLease)
	raftConf.RaftBaseTickInterval = config.ParseDuration(conf.RaftStore.RaftBaseTickInterval)
	raftConf.RaftHeartbeatTicks = conf.RaftStore.RaftHeartbeatTicks
	raftConf.RaftElectionTimeoutTicks = conf.RaftStore.RaftElectionTimeoutTicks

	raftConf.SplitCheck.RegionMaxSize = uint64(conf.Server.RegionSize)
	raftConf.ApplyWorkerCnt = conf.RaftStore.ApplyWorkerCount
	raftConf.GrpcRaftConnNum = uint64(conf.RaftStore.GRPCRaftConnNum)
	raftConf.StatusAddr = conf.Server.StatusAddr
	raftConf.GitHash = conf.RaftStore.GitHash
	for key, value := range conf.Server.Labels {
		raftConf.Labels = append(raftConf.Labels, raftstore.StoreLabel{key, value})
	}
}

func createRaftEngine(subPath string, conf *config.RaftEngine) (*raftengine.Engine, error) {
	return raftengine.Open(filepath.Join(conf.Path, subPath), conf.WALSize)
}

func createKVEngine(subPath string,
	listener *raftstore.MetaChangeListener,
	allocator engine.IDAllocator,
	recoverHandler *raftstore.RecoverHandler,
	pd pd.Client,
	conf *config.Engine,
	storeAddr string) (*engine.Engine, error) {
	opts := engine.DefaultOpt
	opts.BaseSize = conf.BaseSize
	opts.TableBuilderOptions.MaxTableSize = conf.MaxTableSize
	opts.MaxMemTableSizeFactor = conf.MaxMemTableSizeFactor
	opts.RemoteCompactionAddr = conf.RemoteCompactionAddr
	opts.MaxBlockCacheSize = conf.BlockCacheSize
	opts.NumCompactors = conf.NumCompactors
	opts.InstanceID = conf.InstanceID
	opts.RecoveryConcurrency = conf.RecoveryConcurrency
	opts.PreparationConcurrency = conf.PreparationConcurrency

	opts.Dir = filepath.Join(conf.Path, subPath)
	if allocator != nil {
		opts.IDAllocator = allocator
	}
	opts.MetaChangeListener = listener
	opts.RecoverHandler = recoverHandler
	opts.MetaReader = recoverHandler
	var fs dfs.DFS
	var err error
	if conf.S3.KeyID != "" {
		fs, err = s3dfs.NewS3DFS(opts.Dir, opts.InstanceID, conf.S3)
		if err != nil {
			return nil, err
		}
	} else {
		fs = builtindfs.NewBuiltinDFS(pd, opts.Dir, storeAddr)
	}
	return engine.OpenEngine(fs, opts)
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
	err              error
	buf              []byte
	reader           *enginereader.Reader
	method           string
	startTime        time.Time
	rpcCtx           *kvrpcpb.Context
	asyncMinCommitTS uint64
	onePCCommitTS    uint64
	shard            *engine.Shard
}

func newRequestCtx(svr *Server, ctx *kvrpcpb.Context, method string) (*requestCtx, error) {
	req := &requestCtx{
		svr:       svr,
		method:    method,
		startTime: time.Now(),
		rpcCtx:    ctx,
	}
	req.regCtx, req.regErr = svr.regionManager.GetRegionFromCtx(ctx)
	if req.regErr != nil {
		return req, nil
	}
	shd := svr.mvccStore.eng.GetShard(req.rpcCtx.RegionId)
	req.shard = shd
	if shd.Ver != ctx.RegionEpoch.Version {
		meta := req.regCtx.meta
		var startKey, endKey []byte
		if len(shd.Start) != 0 {
			startKey = codec.EncodeBytes(nil, shd.Start)
		}
		if bytes.Compare(shd.End, engine.GlobalShardEndKey) != 0 {
			endKey = codec.EncodeBytes(nil, shd.End)
		}
		req.regErr = &errorpb.Error{
			Message: "stale epoch",
			EpochNotMatch: &errorpb.EpochNotMatch{
				CurrentRegions: []*metapb.Region{{
					Id:          meta.Id,
					StartKey:    startKey,
					EndKey:      endKey,
					RegionEpoch: &metapb.RegionEpoch{Version: shd.Ver, ConfVer: meta.RegionEpoch.ConfVer},
					Peers:       meta.Peers,
				}},
			},
		}
	}
	return req, nil
}

// For read-only requests that doesn't acquire latches, this function must be called after all locks has been checked.
func (req *requestCtx) getKVReader() *enginereader.Reader {
	if req.reader == nil {
		shd := req.shard
		snap := req.svr.mvccStore.eng.NewSnapAccess(shd)
		req.reader = enginereader.NewReader(req.regCtx.startKey, req.regCtx.endKey, snap)
	}
	return req.reader
}

func (req *requestCtx) finish() {
	if req.reader != nil {
		req.reader.Close()
	}
	if req.regErr != nil || req.err != nil {
		metrics.GrpcMsgFailTotal.WithLabelValues(metricsName(req.method)).Inc()
	} else {
		metrics.GrpcMsgDurationSeconds.WithLabelValues(metricsName(req.method)).Observe(float64(time.Now().Sub(req.startTime)) / float64(time.Second))
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
	err = svr.mvccStore.CheckKeysLock(reqCtx, req.GetVersion(), req.Context.ResolvedLocks, req.Key)
	reqCtx.err = err
	if err != nil {
		return &kvrpcpb.GetResponse{Error: convertToKeyError(err)}, nil
	}
	reader := reqCtx.getKVReader()
	val, err := reader.Get(req.Key, req.GetVersion())
	reqCtx.err = err
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
	reqCtx.err = err
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
			LockTS:          errLocked.Lock.StartTS,
			DeadlockKeyHash: result.DeadlockResp.DeadlockKeyHash,
		}
		resp.Errors, resp.RegionError = convertToPBErrors(deadlockErr)
		return resp, nil
	}
	if result.WakeupSleepTime == lockwaiter.WakeUpThisWaiter {
		if req.Force {
			req.WaitTimeout = lockwaiter.LockNoWait
			_, err := svr.mvccStore.PessimisticLock(reqCtx, req, resp)
			reqCtx.err = err
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
	reqCtx.err = err
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
	reqCtx.err = err
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
	reqCtx.err = err
	log.S().Infof("check txn status lockTS:%d commitTS:%d action:%s hasLock:%v", req.LockTs, txnStatus.commitTS, txnStatus.action, txnStatus.lockInfo != nil)
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
	reqCtx.err = err
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
	reqCtx.err = err
	resp := &kvrpcpb.PrewriteResponse{}
	if reqCtx.asyncMinCommitTS > 0 {
		resp.MinCommitTs = reqCtx.asyncMinCommitTS
	}
	if reqCtx.onePCCommitTS > 0 {
		resp.OnePcCommitTs = reqCtx.onePCCommitTS
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
	reqCtx.err = err
	if err != nil {
		resp.Error, resp.RegionError = convertToPBError(err)
	}
	return resp, nil
}

// RawGetKeyTTL implements implements the tikvpb.TikvServer interface.
func (svr *Server) RawGetKeyTTL(ctx context.Context, req *kvrpcpb.RawGetKeyTTLRequest) (*kvrpcpb.RawGetKeyTTLResponse, error) {
	// TODO
	return &kvrpcpb.RawGetKeyTTLResponse{}, nil
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
	reqCtx.err = err
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
	reqCtx.err = err
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
	reqCtx.err = err
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
			reqCtx.err = err
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
	err = svr.mvccStore.writer.DeleteRange(req.StartKey, req.EndKey, reqCtx.regCtx)
	reqCtx.err = err
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
	resp := cophandler.HandleCopRequest(reqCtx.getKVReader(), req)
	return resp, nil
}

func (svr *Server) CoprocessorStream(*coprocessor.Request, tikvpb.Tikv_CoprocessorStreamServer) error {
	// TODO
	return nil
}

func (svr *Server) BatchCoprocessor(*coprocessor.BatchRequest, tikvpb.Tikv_BatchCoprocessorServer) error {
	panic("todo")
}

func (svr *Server) DispatchMPPTask(_ context.Context, _ *mpp.DispatchTaskRequest) (*mpp.DispatchTaskResponse, error) {
	panic("todo")
}

func (svr *Server) CancelMPPTask(_ context.Context, _ *mpp.CancelTaskRequest) (*mpp.CancelTaskResponse, error) {
	panic("todo")
}

func (svr *Server) EstablishMPPConnection(*mpp.EstablishMPPConnectionRequest, tikvpb.Tikv_EstablishMPPConnectionServer) error {
	panic("todo")
}

// Raft commands (tikv <-> tikv).
func (svr *Server) Raft(stream tikvpb.Tikv_RaftServer) error {
	return svr.innerServer.Raft(stream)
}
func (svr *Server) Snapshot(_ tikvpb.Tikv_SnapshotServer) error {
	return nil
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
	if reqCtx.regErr != nil {
		return &kvrpcpb.SplitRegionResponse{RegionError: reqCtx.regErr}, nil
	}
	return svr.regionManager.SplitRegion(req, reqCtx), nil
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
	reqCtx.err = err
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
	reqCtx.err = err
	if err != nil {
		resp.Error = err.Error()
	}
	resp.Info = mvccInfo
	resp.Key = key
	return resp, nil
}

func (svr *Server) UnsafeDestroyRange(ctx context.Context, req *kvrpcpb.UnsafeDestroyRangeRequest) (*kvrpcpb.UnsafeDestroyRangeResponse, error) {
	// TODO
	resp := &kvrpcpb.UnsafeDestroyRangeResponse{}
	return resp, nil
}

// deadlock detection related services
// GetWaitForEntries tries to get the waitFor entries
func (svr *Server) GetWaitForEntries(ctx context.Context,
	req *deadlock.WaitForEntriesRequest) (*deadlock.WaitForEntriesResponse, error) {
	// TODO
	return &deadlock.WaitForEntriesResponse{}, nil
}

// Detect will handle detection rpc from other nodes
func (svr *Server) Detect(stream deadlock.Deadlock_DetectServer) error {
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
	startTime := time.Now()
	resp := &kvrpcpb.PhysicalScanLockResponse{}
	resp.Locks = svr.mvccStore.PhysicalScanLock(req.StartKey, req.MaxTs, int(req.Limit))
	metrics.GrpcMsgDurationSeconds.WithLabelValues("physical_scan_lock").Observe(float64(time.Now().Sub(startTime)) / float64(time.Second))
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

// CheckLeader implements implements the tikvpb.TikvServer interface.
func (svr *Server) CheckLeader(context.Context, *kvrpcpb.CheckLeaderRequest) (*kvrpcpb.CheckLeaderResponse, error) {
	panic("unimplemented")
}

func (svr *Server) RawCompareAndSwap(c context.Context, request *kvrpcpb.RawCASRequest) (*kvrpcpb.RawCASResponse, error) {
	panic("implement me")
}

func (svr *Server) RawCoprocessor(c context.Context, request *kvrpcpb.RawCoprocessorRequest) (*kvrpcpb.RawCoprocessorResponse, error) {
	panic("implement me")
}

func (svr *Server) GetStoreSafeTS(c context.Context, request *kvrpcpb.StoreSafeTSRequest) (*kvrpcpb.StoreSafeTSResponse, error) {
	panic("implement me")
}

func (svr *Server) GetLockWaitInfo(c context.Context, request *kvrpcpb.GetLockWaitInfoRequest) (*kvrpcpb.GetLockWaitInfoResponse, error) {
	panic("implement me")
}

func (svr *Server) ServeRawRaft(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Err.Error() == "use of closed network connection" {
					return nil
				}
			}
			return err
		}
		go svr.innerServer.HandleRawRaft(conn)
	}
}

func convertToKeyError(err error) *kvrpcpb.KeyError {
	if err == nil {
		return nil
	}
	causeErr := errors.Cause(err)
	switch x := causeErr.(type) {
	case *ErrLocked:
		return &kvrpcpb.KeyError{
			Locked: x.Lock.ToLockInfo(x.Key),
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

func metricsName(name string) string {
	buffer := new(bytes.Buffer)
	for i, r := range name {
		if unicode.IsUpper(r) {
			if i != 0 {
				buffer.WriteByte('_')
			}
			buffer.WriteRune(unicode.ToLower(r))
		} else {
			buffer.WriteRune(r)
		}
	}
	return buffer.String()
}
