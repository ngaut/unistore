package raftstore

import (
	"context"
	"sync"

	"github.com/ngaut/unistore/pd"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
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

func (ris *RaftInnerServer) Raft(stream tikvpb.Tikv_RaftServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		ris.raftRouter.SendRaftMessage(msg)
	}
}

func (ris *RaftInnerServer) BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error {
	for {
		msgs, err := stream.Recv()
		if err != nil {
			return err
		}
		for _, msg := range msgs.GetMsgs() {
			ris.raftRouter.SendRaftMessage(msg)
		}
	}
}

func (ris *RaftInnerServer) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	var err error
	done := make(chan struct{})
	ris.snapWorker.scheduler <- task{
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

func (ris *RaftInnerServer) Setup(pdClient pd.Client) {
	var wg sync.WaitGroup
	ris.pdWorker = newWorker("pd-worker", &wg)
	ris.resolveWorker = newWorker("resolver", &wg)
	ris.snapWorker = newWorker("snap-worker", &wg)

	// TODO: create local reader
	// TODO: create storage read pool
	// TODO: create cop read pool
	// TODO: create cop endpoint

	cfg := ris.raftConfig
	router, batchSystem := createRaftBatchSystem(cfg)

	ris.raftRouter = NewRaftstoreRouter(router) // TODO: init with local reader
	ris.snapManager = NewSnapManager(cfg.SnapPath, router)
	ris.batchSystem = batchSystem
	ris.coprocessorHost = newCoprocessorHost(cfg.splitCheck, router)
}

func (ris *RaftInnerServer) GetRaftstoreRouter() *RaftstoreRouter {
	return ris.raftRouter
}

func (ris *RaftInnerServer) GetStoreMeta() *metapb.Store {
	return &ris.storeMeta
}

func (ris *RaftInnerServer) SetPeerEventObserver(ob PeerEventObserver) {
	ris.eventObserver = ob
}

func (ris *RaftInnerServer) Start(pdClient pd.Client) error {
	ris.node = NewNode(ris.batchSystem, &ris.storeMeta, ris.raftConfig, pdClient, ris.eventObserver)

	raftClient := newRaftClient(ris.raftConfig)
	resolveSender := ris.resolveWorker.scheduler
	trans := NewServerTransport(raftClient, ris.snapWorker.scheduler, ris.raftRouter, resolveSender)

	resolveRunner := newResolverRunner(pdClient)
	ris.resolveWorker.start(resolveRunner)
	err := ris.node.Start(context.TODO(), ris.engines, trans, ris.snapManager, ris.pdWorker, ris.coprocessorHost, ris.raftRouter)
	if err != nil {
		return err
	}
	snapRunner := newSnapRunner(ris.snapManager, ris.raftConfig, ris.raftRouter)
	ris.snapWorker.start(snapRunner)
	return nil
}

func (ris *RaftInnerServer) Stop() error {
	ris.snapWorker.stop()
	ris.node.stop()
	ris.resolveWorker.stop()
	if err := ris.engines.raft.Close(); err != nil {
		return err
	}
	if err := ris.engines.kv.db.Close(); err != nil {
		return err
	}
	return nil
}
