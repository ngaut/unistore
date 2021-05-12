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
	"context"
	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/pd"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"sync"
)

type InnerServer struct {
	engines       *Engines
	raftConfig    *Config
	globalConfig  *config.Config
	storeMeta     metapb.Store
	eventObserver PeerEventObserver

	node        *Node
	router      *router
	batchSystem *raftBatchSystem
	pdWorker    *worker
	raftCli     *RaftClient
}

func (ris *InnerServer) Raft(stream tikvpb.Tikv_RaftServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		_ = ris.router.sendRaftMessage(msg)
	}
}

func (ris *InnerServer) BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error {
	for {
		msgs, err := stream.Recv()
		if err != nil {
			return err
		}
		for _, msg := range msgs.GetMsgs() {
			_ = ris.router.sendRaftMessage(msg)
		}
	}
}

func NewRaftInnerServer(globalConfig *config.Config, engines *Engines, raftConfig *Config) *InnerServer {
	return &InnerServer{
		engines:      engines,
		raftConfig:   raftConfig,
		globalConfig: globalConfig,
	}
}

func (ris *InnerServer) Setup(pdClient pd.Client) {
	var wg sync.WaitGroup
	ris.pdWorker = newWorker("pd-worker", &wg)

	// TODO: create local reader
	// TODO: create storage read pool
	// TODO: create cop read pool
	// TODO: create cop endpoint

	cfg := ris.raftConfig
	router, batchSystem := createRaftBatchSystem(ris.globalConfig, cfg)
	ris.engines.listener.initMsgCh(router.storeSender)

	ris.router = router // TODO: init with local reader
	ris.batchSystem = batchSystem
}

func (ris *InnerServer) GetRaftstoreRouter() *RaftstoreRouter {
	return &RaftstoreRouter{router: ris.router}
}

func (ris *InnerServer) GetStoreMeta() *metapb.Store {
	return &ris.storeMeta
}

func (ris *InnerServer) SetPeerEventObserver(ob PeerEventObserver) {
	ris.eventObserver = ob
}

func (ris *InnerServer) Start(pdClient pd.Client) error {
	ris.node = NewNode(ris.batchSystem, &ris.storeMeta, ris.raftConfig, pdClient, ris.eventObserver)

	raftClient := newRaftClient(ris.raftConfig, pdClient)
	err := ris.node.Start(context.TODO(), ris.engines, raftClient, ris.pdWorker, ris.router)
	if err != nil {
		return err
	}
	ris.raftCli = raftClient
	return nil
}

func (ris *InnerServer) Stop() error {
	ris.node.stop()
	ris.raftCli.Stop()
	if err := ris.engines.raft.Close(); err != nil {
		return err
	}
	if err := ris.engines.kv.Close(); err != nil {
		return err
	}
	return nil
}
