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
	"bytes"
	"encoding/binary"

	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/util/codec"
)

// keys
const (
	LocalPrefix byte = 0x01

	// We save two types region data in DB, for raft and other meta data.
	// When the store starts, we should iterate all region meta data to
	// construct peer, no need to travel large raft data, so we separate them
	// with different prefixes.
	RegionRaftPrefix byte = 0x02
	RegionMetaPrefix byte = 0x03
	RegionRaftLogLen      = 19 // REGION_RAFT_PREFIX_KEY + region_id + suffix + index

	// Following are the suffix after the local prefix.
	// For region id
	RaftLogSuffix           byte = 0x01
	RaftStateSuffix         byte = 0x02
	ApplyStateSuffix        byte = 0x03
	SnapshotRaftStateSuffix byte = 0x04

	// For region meta
	RegionStateSuffix byte = 0x01
)

// keys
var (
	MinKey = []byte{0}

	// Data key has two prefix, meta 'm' and table 't',
	// extra keys has prefix 'm' + 1 = 'n',
	// extra table keys has prefix 't' + 1 = 'u', end key would be 'v'.
	MinDataKey = []byte{'m'}
	MaxDataKey = []byte{'v'}

	RegionMetaMinKey = []byte{LocalPrefix, RegionMetaPrefix}
	RegionMetaMaxKey = []byte{LocalPrefix, RegionMetaPrefix + 1}

	// Following keys are all local keys, so the first byte must be 0x01.
	prepareBootstrapKey = []byte{LocalPrefix, 0x01}
	storeIdentKey       = []byte{LocalPrefix, 0x02}
)

func makeRaftRegionPrefix(regionID uint64, suffix byte) []byte {
	key := make([]byte, 11)
	key[0] = LocalPrefix
	key[1] = RegionRaftPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	key[10] = suffix
	return key
}

func makeRaftRegionKey(regionID uint64, suffix byte, subID uint64) []byte {
	key := make([]byte, 19)
	key[0] = LocalPrefix
	key[1] = RegionRaftPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	key[10] = suffix
	binary.BigEndian.PutUint64(key[11:], subID)
	return key
}

// RegionRaftPrefixKey returns the region raft prefix key with the given region id.
func RegionRaftPrefixKey(regionID uint64) []byte {
	key := make([]byte, 10)
	key[0] = LocalPrefix
	key[1] = RegionRaftPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	return key
}

// RaftLogKey makes the raft log key with the given region id and index.
func RaftLogKey(regionID, index uint64) []byte {
	return makeRaftRegionKey(regionID, RaftLogSuffix, index)
}

// RaftStateKey makes the raft state key with the given region id.
func RaftStateKey(regionID uint64) []byte {
	return makeRaftRegionPrefix(regionID, RaftStateSuffix)
}

// ApplyStateKey makes the apply state key with the given region id.
func ApplyStateKey(regionID uint64) []byte {
	return makeRaftRegionPrefix(regionID, ApplyStateSuffix)
}

// SnapshotRaftStateKey makes the snapshot raft state key with the given region id.
func SnapshotRaftStateKey(regionID uint64) []byte {
	return makeRaftRegionPrefix(regionID, SnapshotRaftStateSuffix)
}

func decodeRegionMetaKey(key []byte) (uint64, byte, error) {
	if len(RegionMetaMinKey)+8+1 != len(key) {
		return 0, 0, errors.Errorf("invalid region meta key length for key %v", key)
	}
	if !bytes.HasPrefix(key, RegionMetaMinKey) {
		return 0, 0, errors.Errorf("invalid region meta key prefix for key %v", key)
	}
	regionID := binary.BigEndian.Uint64(key[len(RegionMetaMinKey):])
	return regionID, key[len(key)-1], nil
}

// RegionMetaPrefixKey returns the region meta prefix key with the given region id.
func RegionMetaPrefixKey(regionID uint64) []byte {
	key := make([]byte, 10)
	key[0] = LocalPrefix
	key[1] = RegionMetaPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	return key
}

// RegionStateKey returns the region state key with the given region id.
func RegionStateKey(regionID uint64) []byte {
	key := make([]byte, 11)
	key[0] = LocalPrefix
	key[1] = RegionMetaPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	key[10] = RegionStateSuffix
	return key
}

// RawStartKey gets the `start_key` of current region in encoded form.
func RawStartKey(region *metapb.Region) []byte {
	// only initialized region's start_key can be encoded, otherwise there must be bugs
	// somewhere.
	y.Assert(len(region.Peers) > 0)
	if len(region.StartKey) == 0 {
		// Data starts with 0x01 is used as local key.
		return MinDataKey
	}
	_, decoded, err := codec.DecodeBytes(region.StartKey, nil)
	y.Assert(err == nil)
	return decoded
}

// RawEndKey gets the `end_key` of current region in encoded form.
func RawEndKey(region *metapb.Region) []byte {
	// only initialized region's end_key can be encoded, otherwise there must be bugs
	// somewhere.
	y.Assert(len(region.Peers) > 0)
	if len(region.EndKey) == 0 {
		return MaxDataKey
	}
	_, decoded, err := codec.DecodeBytes(region.EndKey, nil)
	y.Assert(err == nil)
	return decoded
}

// RaftLogIndex gets the log index from raft log key generated by `raft_log_key`.
func RaftLogIndex(key []byte) (uint64, error) {
	if len(key) != RegionRaftLogLen {
		return 0, errors.Errorf("key %v is not a valid raft log key", key)
	}
	return binary.BigEndian.Uint64(key[RegionRaftLogLen-8:]), nil
}
