package server

import (
	"context"
	"sync"

	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/tikv"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/ngaut/unistore/tikv/raftstore"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type Context struct {
	PDClient pd.Client
}

type RaftHandler interface {
	// TODO
}

type InnerServer interface {
	Setup(ctx *Context) error
	Start(ctx *Context) error
	Stop() error
	GetRegionManager() tikv.RegionManager
	GetDBWriter() mvcc.DBWriter
	GetRaftHandler() RaftHandler
}

type StandAlongInnerServer struct {
	bundle     *mvcc.DBBundle
	safePoint  *tikv.SafePoint
	regionOpts tikv.RegionOptions

	regionManager *tikv.StandAloneRegionManager
	dbWriter      mvcc.DBWriter
}

func NewStandAlongInnerServer(bundle *mvcc.DBBundle, safePoint *tikv.SafePoint, regionOpts tikv.RegionOptions) *StandAlongInnerServer {
	return &StandAlongInnerServer{bundle: bundle, safePoint: safePoint, regionOpts: regionOpts}
}

func (sb *StandAlongInnerServer) Setup(ctx *Context) error {
	sb.regionManager = tikv.NewStandAloneRegionManager(sb.bundle.DB, sb.regionOpts, ctx.PDClient)
	sb.dbWriter = tikv.NewDBWriter(sb.bundle, sb.safePoint)
	return nil
}

func (sb *StandAlongInnerServer) Start(ctx *Context) error {
	return nil
}

func (sb *StandAlongInnerServer) Stop() error {
	if err := sb.regionManager.Close(); err != nil {
		return err
	}
	return sb.bundle.DB.Close()
}

func (sb *StandAlongInnerServer) GetRegionManager() tikv.RegionManager {
	return sb.regionManager
}

func (sb *StandAlongInnerServer) GetDBWriter() mvcc.DBWriter {
	return sb.dbWriter
}

func (sb *StandAlongInnerServer) GetRaftHandler() RaftHandler {
	return nil
}

type RaftInnerServer struct {
	engines    *raftstore.Engines
	snapPath   string
	raftConfig *raftstore.Config

	regionManager *tikv.RaftRegionManager
	dbWriter      mvcc.DBWriter

	node            *raftstore.Node
	snapManager     *raftstore.SnapManager
	coprocessorHost *raftstore.CoprocessorHost
	raftRouter      *raftstore.RaftstoreRouter
	pdWorker        *raftstore.Worker
	resolveWorker   *raftstore.Worker
	snapWorker      *raftstore.Worker
}

func NewRaftInnerServer(engines *raftstore.Engines, snapPath string, raftConfig *raftstore.Config) *RaftInnerServer {
	return &RaftInnerServer{engines: engines, snapPath: snapPath, raftConfig: raftConfig}
}

func (sb *RaftInnerServer) Setup(ctx *Context) error {
	var wg sync.WaitGroup
	sb.pdWorker = raftstore.NewWorker("pd-worker", &wg)
	sb.resolveWorker = raftstore.NewWorker("resolver", &wg)
	sb.snapWorker = raftstore.NewWorker("snap-worker", &wg)

	// TODO: create local reader
	// TODO: create storage read pool
	// TODO: create cop read pool
	// TODO: create cop endpoint

	cfg := sb.raftConfig
	router, batchSystem := raftstore.CreateRaftBatchSystem(cfg)
	sb.raftRouter = raftstore.NewRaftstoreRouter(router) // TODO: init with local reader
	sb.snapManager = raftstore.NewSnapManager(cfg.SnapPath, router)

	var store metapb.Store
	sb.regionManager = tikv.NewRaftRegionManager(&store, sb.raftRouter)
	sb.node = raftstore.NewNode(batchSystem, &store, cfg, ctx.PDClient, sb.regionManager)
	sb.coprocessorHost = raftstore.NewCoprocessorHost(cfg.SplitCheck, router)

	sb.dbWriter = raftstore.NewDBWriter(router)

	return nil
}

func (sb *RaftInnerServer) Start(ctx *Context) error {
	raftClient := raftstore.NewRaftClient(sb.raftConfig)
	resolveSender := sb.resolveWorker.GetScheduler()
	trans := raftstore.NewServerTransport(raftClient, sb.snapWorker.GetScheduler(), sb.raftRouter, resolveSender)

	resolveRunner := raftstore.NewResolverRunner(ctx.PDClient)
	sb.resolveWorker.Start(resolveRunner)
	err := sb.node.Start(context.TODO(), sb.engines, trans, sb.snapManager, sb.pdWorker, sb.coprocessorHost)
	if err != nil {
		return err
	}
	snapRunner := raftstore.NewSnapRunner(sb.snapManager, sb.raftConfig, sb.raftRouter)
	sb.snapWorker.Start(snapRunner)
	return nil
}

func (sb *RaftInnerServer) Stop() error {
	sb.snapWorker.Stop()
	sb.node.Stop()
	return nil
}

func (sb *RaftInnerServer) GetRegionManager() tikv.RegionManager {
	return sb.regionManager
}

func (sb *RaftInnerServer) GetDBWriter() mvcc.DBWriter {
	return sb.dbWriter
}

func (sb *RaftInnerServer) GetRaftHandler() RaftHandler {
	// TODO
	return nil
}
