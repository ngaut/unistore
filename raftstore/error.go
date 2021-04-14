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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
)

// ErrNotLeader is returned when this region is not Leader.
type ErrNotLeader struct {
	RegionID uint64
	Leader   *metapb.Peer
}

func (e *ErrNotLeader) Error() string {
	return fmt.Sprintf("region %v is not leader", e.RegionID)
}

// ErrRegionNotFound is returned when this region is not found.
type ErrRegionNotFound struct {
	RegionID uint64
}

func (e *ErrRegionNotFound) Error() string {
	return fmt.Sprintf("region %v is not found", e.RegionID)
}

// ErrKeyNotInRegion is returned when this key is not in the region.
type ErrKeyNotInRegion struct {
	Key    []byte
	Region *metapb.Region
}

func (e *ErrKeyNotInRegion) Error() string {
	return fmt.Sprintf("key %v is not in region %v", e.Key, e.Region)
}

// ErrEpochNotMatch is returned when this epoch is not match.
type ErrEpochNotMatch struct {
	Message string
	Regions []*metapb.Region
}

func (e *ErrEpochNotMatch) Error() string {
	return fmt.Sprintf("epoch not match, error msg %v, regions %v", e.Message, e.Regions)
}

// ErrServerIsBusy is returned when the server is busy.
type ErrServerIsBusy struct {
	Reason    string
	BackoffMs uint64
}

func (e *ErrServerIsBusy) Error() string {
	return fmt.Sprintf("server is busy, reason %v, backoff ms %v", e.Reason, e.BackoffMs)
}

// ErrStaleCommand is returned when the command is stale.
type ErrStaleCommand struct{}

func (e *ErrStaleCommand) Error() string {
	return "stale command"
}

// ErrStoreNotMatch is returned when the store is not match.
type ErrStoreNotMatch struct {
	RequestStoreID uint64
	ActualStoreID  uint64
}

func (e *ErrStoreNotMatch) Error() string {
	return fmt.Sprintf("store not match, request store id is %v, but actual store id is %v", e.RequestStoreID, e.ActualStoreID)
}

// ErrRaftEntryTooLarge is returned when the raft entry is too large.
type ErrRaftEntryTooLarge struct {
	RegionID  uint64
	EntrySize uint64
}

func (e *ErrRaftEntryTooLarge) Error() string {
	return fmt.Sprintf("raft entry too large, region_id: %v, len: %v", e.RegionID, e.EntrySize)
}

// ErrToPbError converts error to *errorpb.Error.
func ErrToPbError(e error) *errorpb.Error {
	ret := new(errorpb.Error)
	switch err := errors.Cause(e).(type) {
	case *ErrNotLeader:
		ret.NotLeader = &errorpb.NotLeader{RegionId: err.RegionID, Leader: err.Leader}
	case *ErrRegionNotFound:
		ret.RegionNotFound = &errorpb.RegionNotFound{RegionId: err.RegionID}
	case *ErrKeyNotInRegion:
		ret.KeyNotInRegion = &errorpb.KeyNotInRegion{Key: err.Key, RegionId: err.Region.Id,
			StartKey: err.Region.StartKey, EndKey: err.Region.EndKey}
	case *ErrEpochNotMatch:
		ret.EpochNotMatch = &errorpb.EpochNotMatch{CurrentRegions: err.Regions}
	case *ErrServerIsBusy:
		ret.ServerIsBusy = &errorpb.ServerIsBusy{Reason: err.Reason, BackoffMs: err.BackoffMs}
	case *ErrStaleCommand:
		ret.StaleCommand = &errorpb.StaleCommand{}
	case *ErrStoreNotMatch:
		ret.StoreNotMatch = &errorpb.StoreNotMatch{RequestStoreId: err.RequestStoreID, ActualStoreId: err.ActualStoreID}
	case *ErrRaftEntryTooLarge:
		ret.RaftEntryTooLarge = &errorpb.RaftEntryTooLarge{RegionId: err.RegionID, EntrySize: err.EntrySize}
	default:
		ret.Message = e.Error()
	}
	return ret
}
