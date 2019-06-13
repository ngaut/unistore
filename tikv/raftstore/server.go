package raftstore

import (
	"context"
	"net"
	"os"
	"sync"

	"github.com/ngaut/log"
	"github.com/ngaut/unistore/pd"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
)

type dummyEventObserver struct {}

func (*dummyEventObserver) OnPeerCreate(ctx *PeerEventContext, region *metapb.Region) {}

func (*dummyEventObserver) OnPeerApplySnap(ctx *PeerEventContext, region *metapb.Region) {}

func (*dummyEventObserver) OnPeerDestroy(ctx *PeerEventContext) {}

func (*dummyEventObserver) OnSplitRegion(derived *metapb.Region, regions []*metapb.Region, peers []*PeerEventContext) {
}

func RunRaftServer(cfg *Config, pdClient pd.Client, engines *Engines, signalChan <-chan os.Signal) error {
	var wg sync.WaitGroup
	pdWorker := NewWorker("pd-worker", &wg)
	resolveWorker := NewWorker("resolver", &wg)
	resolveRunner := NewResolverRunner(pdClient)
	resolveSender := resolveWorker.scheduler

	// TODO: create local reader
	// TODO: create storage read pool
	// TODO: create cop read pool
	// TODO: create cop endpoint

	router, batchSystem := CreateRaftBatchSystem(cfg)
	raftRouter := NewRaftstoreRouter(router) // TODO: init with local reader
	snapManager := NewSnapManager(cfg.SnapPath, router)
	var store metapb.Store
	node := NewNode(batchSystem, &store, cfg, pdClient, new(dummyEventObserver)) // TODO: Add PeerEventObserver

	// TODO: create storage

	server := NewServer(cfg, raftRouter, resolveSender, snapManager)

	coprocessorHost := NewCoprocessorHost(cfg.SplitCheck, router)

	resolveWorker.Start(resolveRunner)

	err := node.Start(context.TODO(), engines, server.Trans(), snapManager, pdWorker, coprocessorHost)
	if err != nil {
		return err
	}

	err = server.Start()
	if err != nil {
		return err
	}

	<-signalChan

	// TODO: Be graceful!
	os.Exit(0)

	err = server.Stop()
	if err != nil {
		log.Errorf("failed to stop server: %v", err)
	}

	node.Stop()

	resolveWorker.Stop()

	wg.Wait()
	return nil
}

type Server struct {
	config      *Config
	wg          *sync.WaitGroup
	snapWorker  *Worker
	grpcServer  *grpc.Server
	trans       *ServerTransport
	snapManager *SnapManager
	lis         net.Listener
}

func NewServer(config *Config, router *RaftstoreRouter, resovleSender chan<- task, snapManager *SnapManager) *Server {
	var wg sync.WaitGroup
	snapWorker := NewWorker("snap-worker", &wg)
	kvService := NewKVService(router, snapWorker.scheduler)

	grpcOpts := []grpc.ServerOption{
		grpc.InitialConnWindowSize(2 * 1024 * 1024),
		grpc.MaxConcurrentStreams(1024),
		grpc.MaxRecvMsgSize(10 * 1024 * 1024),
		grpc.MaxSendMsgSize(1 << 32),
	}
	grpcServer := grpc.NewServer(grpcOpts...)
	tikvpb.RegisterTikvServer(grpcServer, kvService)

	raftClient := NewRaftClient(config)
	trans := NewServerTransport(raftClient, snapWorker.scheduler, router, resovleSender)

	return &Server{
		config:      config,
		wg:          &wg,
		snapWorker:  snapWorker,
		grpcServer:  grpcServer,
		trans:       trans,
		snapManager: snapManager,
	}
}

func (s *Server) Start() error {
	snapRunner := NewSnapRunner(s.snapManager, s.config, s.trans.raftRouter)
	s.snapWorker.Start(snapRunner)
	lis, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		return err
	}
	s.lis = lis
	go func() { s.grpcServer.Serve(lis) }()
	log.Info("tikv is ready to serve")
	return nil
}

func (s *Server) Trans() *ServerTransport {
	return s.trans
}

func (s *Server) Stop() error {
	s.snapWorker.Stop()
	s.grpcServer.Stop()
	return s.lis.Close()
}
