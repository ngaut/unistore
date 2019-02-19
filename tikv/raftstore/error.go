package raftstore

import (
	"fmt"
	"github.com/pingcap/kvproto/pkg/metapb"
)

const (
	ErrStaleCommand = 1
)

type RaftStoreError struct {
	ErrType int
}

func NewStaleCommandErr() *RaftStoreError {
	return &RaftStoreError {
		ErrType: ErrStaleCommand,
	}
}

type ErrRaftEntryTooLarge struct {
	RegionId uint64
	DataLen uint64
}

func (e *ErrRaftEntryTooLarge) Error() string {
	fmt.Sprintf("raft entry too large, region_id: %v, len: %v", e.RegionId, e.DataLen)
}

type ErrNotLeader struct {
	RegionId uint64
}

func (e *ErrNotLeader) Error() string {
	fmt.Sprintf("region %v is not leader", e.RegionId)
}

type ErrKeyNotInRegion struct {
	Key []byte
	Region *metapb.Region
}

func (e *ErrKeyNotInRegion) Error() string {
	fmt.Sprintf("key %v is not in region %v", e.Key, e.Region)
}
