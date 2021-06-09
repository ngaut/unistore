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

import (
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
)

type RaftLog interface {
	RegionID() uint64
	Epoch() Epoch
	PeerID() uint64
	StoreID() uint64
	Term() uint64
	Marshal() []byte
	GetRaftCmdRequest() *raft_cmdpb.RaftCmdRequest
}

func DecodeLog(entryData []byte) RaftLog {
	var rlog RaftLog
	if entryData[0] == CustomRaftLogFlag {
		rlog = NewCustom(entryData)
	} else {
		cmd := new(raft_cmdpb.RaftCmdRequest)
		err := cmd.Unmarshal(entryData)
		if err != nil {
			panic(err)
		}
		rlog = NewRequest(cmd)
	}
	return rlog
}
