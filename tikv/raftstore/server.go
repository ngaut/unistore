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

type RaftInnerServer struct {
	engines       *Engines
	raftConfig    *Config
	storeMeta     metapb.Store
	eventObserver PeerEventObserver

	node            *Node

	snapManager     *SnapManager
	coprocessorHost *CoprocessorHost
	raftRouter      *RaftstoreRouter
	batchSystem     *raftBatchSystem
	pdWorker        *worker
	resolveWorker   *worker
	snapWorker      *worker
}

func (sb *RaftInnerServer) Raft(stream tikvpb.Tikv_RaftServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		sb.raftRouter.SendRaftMessage(msg)
	}
}

func (sb *RaftInnerServer) BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error {
	for {
		msgs, err := stream.Recv()
		if err != nil {
			return err
		}
		for _, msg := range msgs.GetMsgs() {
			sb.raftRouter.SendRaftMessage(msg)
		}
	}
}

func (sb *RaftInnerServer) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	var err error
	done := make(chan struct{})
	sb.snapWorker.scheduler <- task{
		tp: taskTypeSnapRecv,
		data: recvSnapTask{
			stream: stream,
			callback: func(e error) {
				err = e
				close(done)
			},
		},
	}
	<-done
	return err
}

func NewRaftInnerServer(engines *Engines, raftConfig *Config) *RaftInnerServer {
	return &RaftInnerServer{engines: engines, raftConfig: raftConfig}
}

func (sb *RaftInnerServer) Setup(pdClient pd.Client) {
	var wg sync.WaitGroup
	sb.pdWorker = newWorker("pd-worker", &wg)
	sb.resolveWorker = newWorker("resolver", &wg)
	sb.snapWorker = newWorker("snap-worker", &wg)

	// TODO: create local reader
	// TODO: create storage read pool
	// TODO: create cop read pool
	// TODO: create cop endpoint

	cfg := sb.raftConfig
	router, batchSystem := createRaftBatchSystem(cfg)

	sb.raftRouter = NewRaftstoreRouter(router) // TODO: init with local reader
	sb.snapManager = NewSnapManager(cfg.SnapPath, router)
	sb.batchSystem = batchSystem
	sb.coprocessorHost = newCoprocessorHost(cfg.splitCheck, router)
}

func (sb *RaftInnerServer) GetRaftstoreRouter() *RaftstoreRouter {
	return sb.raftRouter
}

func (sb *RaftInnerServer) GetStoreMeta() *metapb.Store {
	return &sb.storeMeta
}

func (sb *RaftInnerServer) SetPeerEventObserver(ob PeerEventObserver) {
	sb.eventObserver = ob
}

func (sb *RaftInnerServer) Start(pdClient pd.Client) error {
	sb.node = NewNode(sb.batchSystem, &sb.storeMeta, sb.raftConfig, pdClient, sb.eventObserver)

	raftClient := newRaftClient(sb.raftConfig)
	resolveSender := sb.resolveWorker.scheduler
	trans := NewServerTransport(raftClient, sb.snapWorker.scheduler, sb.raftRouter, resolveSender)

	resolveRunner := newResolverRunner(pdClient)
	sb.resolveWorker.start(resolveRunner)
	err := sb.node.Start(context.TODO(), sb.engines, trans, sb.snapManager, sb.pdWorker, sb.coprocessorHost)
	if err != nil {
		return err
	}
	snapRunner := newSnapRunner(sb.snapManager, sb.raftConfig, sb.raftRouter)
	sb.snapWorker.start(snapRunner)
	return nil
}

func (sb *RaftInnerServer) Stop() error {
	sb.snapWorker.stop()
	sb.node.stop()
	return nil
}

type dummyEventObserver struct {}

func (*dummyEventObserver) OnPeerCreate(ctx *PeerEventContext, region *metapb.Region) {}

func (*dummyEventObserver) OnPeerApplySnap(ctx *PeerEventContext, region *metapb.Region) {}

func (*dummyEventObserver) OnPeerDestroy(ctx *PeerEventContext) {}

func (*dummyEventObserver) OnSplitRegion(derived *metapb.Region, regions []*metapb.Region, peers []*PeerEventContext) {}

func RunRaftServer(cfg *Config, pdClient pd.Client, engines *Engines, signalChan <-chan os.Signal) error {
	var wg sync.WaitGroup
	pdWorker := newWorker("pd-worker", &wg)
	resolveWorker := newWorker("resolver", &wg)
	resolveRunner := newResolverRunner(pdClient)
	resolveSender := resolveWorker.scheduler

	// TODO: create local reader
	// TODO: create storage read pool
	// TODO: create cop read pool
	// TODO: create cop endpoint

	router, batchSystem := createRaftBatchSystem(cfg)
	raftRouter := NewRaftstoreRouter(router) // TODO: init with local reader
	snapManager := NewSnapManager(cfg.SnapPath, router)
	var store metapb.Store
	node := NewNode(batchSystem, &store, cfg, pdClient, new(dummyEventObserver)) // TODO: Add PeerEventObserver

	// TODO: create storage

	server := NewServer(cfg, raftRouter, resolveSender, snapManager)

	coprocessorHost := newCoprocessorHost(cfg.splitCheck, router)

	resolveWorker.start(resolveRunner)

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

	node.stop()

	resolveWorker.stop()

	wg.Wait()
	return nil
}

type Server struct {
	config      *Config
	wg          *sync.WaitGroup
	snapWorker  *worker
	grpcServer  *grpc.Server
	trans       *ServerTransport
	snapManager *SnapManager
	lis         net.Listener
}

func NewServer(config *Config, router *RaftstoreRouter, resovleSender chan<- task, snapManager *SnapManager) *Server {
	var wg sync.WaitGroup
	snapWorker := newWorker("snap-worker", &wg)
	kvService := NewKVService(router, snapWorker.scheduler)

	grpcOpts := []grpc.ServerOption{
		grpc.InitialConnWindowSize(2 * 1024 * 1024),
		grpc.MaxConcurrentStreams(1024),
		grpc.MaxRecvMsgSize(10 * 1024 * 1024),
		grpc.MaxSendMsgSize(1 << 32),
	}
	grpcServer := grpc.NewServer(grpcOpts...)
	tikvpb.RegisterTikvServer(grpcServer, kvService)

	raftClient := newRaftClient(config)
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
	snapRunner := newSnapRunner(s.snapManager, s.config, s.trans.raftRouter)
	s.snapWorker.start(snapRunner)
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
	s.snapWorker.stop()
	s.grpcServer.Stop()
	return s.lis.Close()
}
