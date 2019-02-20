package raftstore

import (
	"fmt"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type ErrNotLeader struct {
	RegionId uint64
	Leader *metapb.Peer
}

func (e *ErrNotLeader) Error() string {
	return fmt.Sprintf("region %v is not leader", e.RegionId)
}

type ErrRegionNotFound struct {
	RegionId uint64
}

func (e *ErrRegionNotFound) Error() string {
	return fmt.Sprintf("region %v is not found", e.RegionId)
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

type ErrServerIsBusy struct {
	Reason string
	BackoffMs uint64
}

func (e *ErrServerIsBusy) Error() string {
	return fmt.Sprintf("server is busy, reason %v, backoff ms %v", e.Reason, e.BackoffMs)
}

type ErrStaleCommand struct {}

func (e *ErrStaleCommand) Error() string {
	return fmt.Sprintf("stale command")
}

type ErrStoreNotMatch struct {
	RequestStoreId uint64
	ActualStoreId uint64
}

func (e *ErrStoreNotMatch) Error() string {
	return fmt.Sprintf("store not match, request store id is %v, but actual store id is %v", e.RequestStoreId, e.ActualStoreId)
}

type ErrRaftEntryTooLarge struct {
	RegionId uint64
	EntrySize uint64
}

func (e *ErrRaftEntryTooLarge) Error() string {
	return fmt.Sprintf("raft entry too large, region_id: %v, len: %v", e.RegionId, e.EntrySize)
}
