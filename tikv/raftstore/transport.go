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
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/log"
	"github.com/zhangjinpeng1987/raft"
	"go.uber.org/zap"
)

type ServerTransport struct {
	raftClient    *RaftClient
	router        *router
	snapScheduler chan<- task
}

func NewServerTransport(raftClient *RaftClient, snapScheduler chan<- task, router *router) *ServerTransport {
	return &ServerTransport{
		raftClient:    raftClient,
		router:        router,
		snapScheduler: snapScheduler,
	}
}

func (t *ServerTransport) Send(msg *raft_serverpb.RaftMessage) error {
	if msg.GetMessage().GetSnapshot() != nil {
		t.SendSnapshotSock(msg)
	} else {
		t.raftClient.Send(msg)
	}
	return nil
}

func (t *ServerTransport) SendSnapshotSock(msg *raft_serverpb.RaftMessage) {
	callback := func(err error) {
		if err != nil {
			t.ReportSnapshotStatus(msg, raft.SnapshotFailure)
		} else {
			t.ReportSnapshotStatus(msg, raft.SnapshotFinish)
		}
	}

	task := task{
		tp: taskTypeSnapSend,
		data: sendSnapTask{
			storeID:  msg.GetToPeer().GetStoreId(),
			msg:      msg,
			callback: callback,
		},
	}
	t.snapScheduler <- task
}

func (t *ServerTransport) ReportSnapshotStatus(msg *raft_serverpb.RaftMessage, status raft.SnapshotStatus) {
	regionID := msg.GetRegionId()
	toPeerID := msg.GetToPeer().GetId()
	toStoreID := msg.GetToPeer().GetStoreId()
	log.Debug("send snapshot", zap.Uint64("to peer", toPeerID), zap.Uint64("region id", regionID), zap.Int("status", int(status)))
	if err := t.router.send(regionID, NewMsg(MsgTypeSignificantMsg, &MsgSignificant{
		Type:           MsgSignificantTypeStatus,
		ToPeerID:       toPeerID,
		SnapshotStatus: status,
	})); err != nil {
		log.Error("report snapshot to peer fails", zap.Uint64("to peer", toPeerID), zap.Uint64("to store", toStoreID), zap.Uint64("region id", regionID), zap.Error(err))
	}
}

func (t *ServerTransport) ReportUnreachable(msg *raft_serverpb.RaftMessage) {
	regionID := msg.GetRegionId()
	toPeerID := msg.GetToPeer().GetId()
	toStoreID := msg.GetToPeer().GetStoreId()
	if msg.GetMessage().GetMsgType() == eraftpb.MessageType_MsgSnapshot {
		t.ReportSnapshotStatus(msg, raft.SnapshotFailure)
		return
	}
	if err := t.router.send(regionID, NewMsg(MsgTypeSignificantMsg, &MsgSignificant{
		Type:     MsgSignificantTypeUnreachable,
		ToPeerID: toPeerID,
	})); err != nil {
		log.Error("report peer unreachable failed", zap.Uint64("to peer", toPeerID), zap.Uint64("to store", toStoreID), zap.Uint64("region id", regionID), zap.Error(err))
	}
}

func (t *ServerTransport) Flush() {
	t.raftClient.Flush()
}
