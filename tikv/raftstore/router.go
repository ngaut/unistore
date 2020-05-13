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
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/unistore/tikv/raftstore/raftlog"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"

	"github.com/pingcap/kvproto/pkg/raft_serverpb"

	"github.com/pingcap/errors"
)

// router routes a message to a peer.
type router struct {
	peers       sync.Map
	peerSender  chan Msg
	storeSender chan<- Msg
	storeFsm    *storeFsm
}

func newRouter(storeSender chan<- Msg, storeFsm *storeFsm) *router {
	pm := &router{
		peerSender:  make(chan Msg, 4096),
		storeSender: storeSender,
		storeFsm:    storeFsm,
	}
	return pm
}

func (pr *router) get(regionID uint64) *peerState {
	v, ok := pr.peers.Load(regionID)
	if ok {
		return v.(*peerState)
	}
	return nil
}

func (pr *router) register(peer *peerFsm) {
	id := peer.peer.regionId
	apply := newApplierFromPeer(peer)
	newPeer := &peerState{
		peer:  peer,
		apply: apply,
	}
	pr.peers.Store(id, newPeer)
}

func (pr *router) close(regionID uint64) {
	v, ok := pr.peers.Load(regionID)
	if ok {
		ps := v.(*peerState)
		atomic.StoreUint32(&ps.closed, 1)
		pr.peers.Delete(regionID)
	}
}

func (pr *router) send(regionID uint64, msg Msg) error {
	msg.RegionID = regionID
	p := pr.get(regionID)
	if p == nil || atomic.LoadUint32(&p.closed) == 1 {
		return errPeerNotFound
	}
	pr.peerSender <- msg
	return nil
}

func (pr *router) sendRaftCommand(cmd *MsgRaftCmd) error {
	regionID := cmd.Request.RegionID()
	return pr.send(regionID, NewPeerMsg(MsgTypeRaftCmd, regionID, cmd))
}

func (pr *router) sendRaftMessage(msg *raft_serverpb.RaftMessage) error {
	regionID := msg.RegionId
	if pr.send(regionID, NewPeerMsg(MsgTypeRaftMessage, regionID, msg)) != nil {
		pr.sendStore(NewPeerMsg(MsgTypeStoreRaftMessage, regionID, msg))
	}
	return nil
}

func (pr *router) sendStore(msg Msg) {
	pr.storeSender <- msg
}

// RaftstoreRouter exports SendCommand method for other packages.
type RaftstoreRouter struct {
	router *router
	// TODO: add localReader here.
}

func (r *RaftstoreRouter) SendCommand(req *raft_cmdpb.RaftCmdRequest, cb *Callback) error {
	// TODO: support local reader
	msg := &MsgRaftCmd{
		SendTime: time.Now(),
		Request:  raftlog.NewRequest(req),
		Callback: cb,
	}
	return r.router.sendRaftCommand(msg)
}

func (r *RaftstoreRouter) SplitRegion(ctx *kvrpcpb.Context, keys [][]byte) ([]*metapb.Region, error) {
	cb := NewCallback()
	msg := &MsgSplitRegion{
		RegionEpoch: ctx.RegionEpoch,
		SplitKeys:   keys,
		Callback:    cb,
	}
	err := r.router.send(ctx.RegionId, Msg{Type: MsgTypeSplitRegion, Data: msg})
	if err != nil {
		return nil, err
	}
	cb.wg.Wait()
	return cb.resp.GetAdminResponse().GetSplits().GetRegions(), nil
}

var errPeerNotFound = errors.New("peer not found")
