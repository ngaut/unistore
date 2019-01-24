package raftstore

import (
	"encoding/binary"
	"github.com/coocood/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/codec"
)

const (
	// local is in (0x01, 0x02)
	LocalPrefix byte = 0x01
	DataPrefix  byte = 'z'

	// We save two types region data in DB, for raft and other meta data.
	// When the store starts, we should iterate all region meta data to
	// construct peer, no need to travel large raft data, so we separate them
	// with different prefixes.
	RegionRaftPrefix    byte = 0x02
	RegionMetaPrefix    byte = 0x03
	RegionRaftPrefixLen      = 11 // REGION_RAFT_PREFIX_KEY + region_id + suffix
	RegionRaftLogLen         = 19 // REGION_RAFT_PREFIX_KEY + region_id + suffix + index

	// Following are the suffix after the local prefix.
	// For region id
	RaftLogSuffix           byte = 0x01
	RaftStateSuffix         byte = 0x02
	ApplyStateSuffix        byte = 0x03
	SnapshotRaftStateSuffix byte = 0x04

	// For region meta
	RegionStateSuffix byte = 0x01
)

var (
	MinKey           = []byte{}
	MaxKey           = []byte{255}
	LocalMinKey      = []byte{LocalPrefix}
	LocalMaxKey      = []byte{LocalPrefix + 1}
	DataMinKey       = []byte{DataPrefix}
	DataMaxKey       = []byte{DataPrefix + 1}
	RegionMetaMinKey = []byte{LocalPrefix, RegionMetaPrefix}
	RegionMetaMaxKey = []byte{LocalPrefix, RegionMetaPrefix + 1}

	// Following keys are all local keys, so the first byte must be 0x01.
	prepareBootstrapKey = []byte{LocalPrefix, 0x01}
	storeIdentKey       = []byte{LocalPrefix, 0x02}
)

func makeRegionPrefix(regionID uint64, suffix byte) []byte {
	key := make([]byte, 11)
	key[0] = LocalPrefix
	key[1] = RegionRaftPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	key[10] = suffix
	return key
}

func makeRegionKey(regionID uint64, suffix byte, subID uint64) []byte {
	key := make([]byte, 19)
	key[0] = LocalPrefix
	key[1] = RegionRaftPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	key[10] = suffix
	binary.BigEndian.PutUint64(key[11:], subID)
	return key
}

func RegionRaftPrefixKey(regionID uint64) []byte {
	key := make([]byte, 10)
	key[0] = LocalPrefix
	key[1] = RegionRaftPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	return key
}

func RaftLogKey(regionID, index uint64) []byte {
	return makeRegionKey(regionID, RaftLogSuffix, index)
}

func RaftStateKey(regionID uint64) []byte {
	return makeRegionPrefix(regionID, RaftStateSuffix)
}

func ApplyStateKey(regionID uint64) []byte {
	return makeRegionPrefix(regionID, ApplyStateSuffix)
}

func RegionMetaPrefixKey(regionID uint64) []byte {
	key := make([]byte, 10)
	key[0] = LocalPrefix
	key[1] = RegionMetaPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	return key
}

func RegionStateKey(regionID uint64) []byte {
	key := make([]byte, 11)
	key[0] = LocalPrefix
	key[1] = RegionMetaPrefix
	binary.BigEndian.PutUint64(key[2:], regionID)
	key[10] = RegionStateSuffix
	return key
}

func ValidateDataKey(key []byte) bool {
	return len(key) > 0 && key[0] == DataPrefix
}

func DataKey(key []byte) []byte {
	v := make([]byte, 0, 1+len(key))
	v = append(v, DataPrefix)
	return append(v, key...)
}

func OriginKey(key []byte) []byte {
	y.AssertTruef(ValidateDataKey(key), "invalid data key %v", key)
	return key[1:]
}

func DataEndKey(key []byte) []byte {
	if len(key) == 0 {
		return DataMinKey
	}
	return DataKey(key)
}

/// RaftLogIndex gets the log index from raft log key generated by `raft_log_key`.
func RaftLogIndex(key []byte) (uint64, error) {
	if len(key) != RegionRaftLogLen {
		return 0, errors.Errorf("key %v is not a valid raft log key", key)
	}
	return binary.BigEndian.Uint64(key[RegionRaftLogLen-8:]), nil
}

func rawRegionKey(key []byte) []byte {
	if len(key) == 0 {
		return key
	}
	_, rawKey, err := codec.DecodeBytes(key, nil)
	y.Assert(err == nil)
	return rawKey
}
