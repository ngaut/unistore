package raftstore

import (
	"fmt"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type ErrStaleCommand struct {
}

func (e *ErrStaleCommand) Error() string {
	return fmt.Sprintf("stale command")
}

type ErrRaftEntryTooLarge struct {
	RegionId uint64
	DataLen uint64
}

func (e *ErrRaftEntryTooLarge) Error() string {
	return fmt.Sprintf("raft entry too large, region_id: %v, len: %v", e.RegionId, e.DataLen)
}

type ErrNotLeader struct {
	RegionId uint64
}

func (e *ErrNotLeader) Error() string {
	return fmt.Sprintf("region %v is not leader", e.RegionId)
}

type ErrKeyNotInRegion struct {
	Key []byte
	Region *metapb.Region
}

func (e *ErrKeyNotInRegion) Error() string {
	return fmt.Sprintf("key %v is not in region %v", e.Key, e.Region)
}

type ErrEpochNotMatch struct {
	Message string
	Regions []*metapb.Region
}

func (e *ErrEpochNotMatch) Error() string {
	return fmt.Sprintf("epoch not match, error msg %v, regions %v", e.Message, e.Regions)
}
