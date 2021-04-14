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

package raftlog

import "github.com/pingcap/kvproto/pkg/raft_cmdpb"

// RequestRaftLog implements the RaftLog interface.
type RequestRaftLog struct {
	*raft_cmdpb.RaftCmdRequest
}

// RegionID implements the RaftLog RegionID method.
func (r RequestRaftLog) RegionID() uint64 {
	return r.Header.RegionId
}

// Epoch implements the RaftLog Epoch method.
func (r RequestRaftLog) Epoch() Epoch {
	e := r.Header.RegionEpoch
	if e == nil {
		return Epoch{}
	}
	return Epoch{ver: uint32(e.Version), confVer: uint32(e.ConfVer)}
}

// RegionConfVer returns the config change version of the region epoch.
func (r RequestRaftLog) RegionConfVer() uint64 {
	return r.Header.RegionEpoch.ConfVer
}

// StoreID implements the RaftLog StoreID method.
func (r RequestRaftLog) StoreID() uint64 {
	return r.Header.Peer.StoreId
}

// PeerID implements the RaftLog PeerID method.
func (r RequestRaftLog) PeerID() uint64 {
	return r.Header.Peer.Id
}

// Term implements the RaftLog Term method.
func (r RequestRaftLog) Term() uint64 {
	return r.Header.Term
}

// GetRaftCmdRequest implements the RaftLog GetRaftCmdRequest method.
func (r RequestRaftLog) GetRaftCmdRequest() *raft_cmdpb.RaftCmdRequest {
	return r.RaftCmdRequest
}

// Marshal implements the RaftLog Marshal method.
func (r RequestRaftLog) Marshal() []byte {
	data, err := r.RaftCmdRequest.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

// NewRequest returns a new RaftLog.
func NewRequest(req *raft_cmdpb.RaftCmdRequest) RaftLog {
	return RequestRaftLog{RaftCmdRequest: req}
}
