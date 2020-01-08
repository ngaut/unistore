package raftlog

import "github.com/pingcap/kvproto/pkg/raft_cmdpb"

type RaftLog interface {
	RegionID() uint64
	Epoch() Epoch
	PeerID() uint64
	StoreID() uint64
	Term() uint64
	Marshal() []byte
	GetRaftCmdRequest() *raft_cmdpb.RaftCmdRequest
}
