package raftlog

import (
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"unsafe"
)

const (
	EngineMetaChangeFlag byte = 65
)

var _ RaftLog = new(EngineMetaChangeRaftLog)

type EngineMetaChangeRaftLog struct {
	header *CustomHeader
	Data   []byte
}

func (c *EngineMetaChangeRaftLog) RegionID() uint64 {
	return c.header.RegionID
}

func (c *EngineMetaChangeRaftLog) PeerID() uint64 {
	return c.header.PeerID
}

func (c *EngineMetaChangeRaftLog) StoreID() uint64 {
	return c.header.StoreID
}

func (c *EngineMetaChangeRaftLog) Epoch() Epoch {
	return c.header.Epoch
}

func (c *EngineMetaChangeRaftLog) Term() uint64 {
	return c.header.Term
}

func (c *EngineMetaChangeRaftLog) Marshal() []byte {
	return c.Data
}

func (c *EngineMetaChangeRaftLog) GetRaftCmdRequest() *raft_cmdpb.RaftCmdRequest {
	return nil
}

func (c *EngineMetaChangeRaftLog) UnmarshalEvent() *protos.MetaChangeEvent {
	e := new(protos.MetaChangeEvent)
	err := e.Unmarshal(c.Data[4+headerSize:])
	y.Assert(err == nil)
	return e
}

func NewEngineMetaChange(data []byte) *EngineMetaChangeRaftLog {
	rlog := &EngineMetaChangeRaftLog{}
	rlog.header = (*CustomHeader)(unsafe.Pointer(&data[4]))
	rlog.Data = data
	return rlog
}

func BuildEngineMetaChangeLog(hdr CustomHeader, e *protos.MetaChangeEvent) RaftLog {
	headerBin := hdr.Marshal()
	data := make([]byte, 4+len(headerBin)+e.Size())
	data[0] = EngineMetaChangeFlag
	copy(data[4:], headerBin)
	_, _ = e.MarshalTo(data[4+len(headerBin):])
	return NewEngineMetaChange(data)
}
