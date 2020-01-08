package raftlog

import "github.com/pingcap/kvproto/pkg/raft_cmdpb"

type RequestRaftLog struct {
	*raft_cmdpb.RaftCmdRequest
}

func (r RequestRaftLog) RegionID() uint64 {
	return r.Header.RegionId
}

func (r RequestRaftLog) Epoch() Epoch {
	e := r.Header.RegionEpoch
	if e == nil {
		return Epoch{}
	}
	return Epoch{ver: uint32(e.Version), confVer: uint32(e.ConfVer)}
}

func (r RequestRaftLog) RegionConfVer() uint64 {
	return r.Header.RegionEpoch.ConfVer
}

func (r RequestRaftLog) StoreID() uint64 {
	return r.Header.Peer.StoreId
}

func (r RequestRaftLog) PeerID() uint64 {
	return r.Header.Peer.Id
}

func (r RequestRaftLog) Term() uint64 {
	return r.Header.Term
}

func (r RequestRaftLog) GetRaftCmdRequest() *raft_cmdpb.RaftCmdRequest {
	return r.RaftCmdRequest
}

func (r RequestRaftLog) Marshal() []byte {
	data, err := r.RaftCmdRequest.Marshal()
	if err != nil {
		panic(err)
	}
	return data
}

func NewRequest(req *raft_cmdpb.RaftCmdRequest) RaftLog {
	return RequestRaftLog{RaftCmdRequest: req}
}
