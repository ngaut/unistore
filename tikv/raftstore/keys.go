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
	"encoding/binary"
	"fmt"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/util/codec"
)

const (
	RaftStateKeyByte        byte = 0x01
	RegionMetaKeyByte       byte = 0x02
	StoreIdentKeyByte       byte = 0x03
	PrepareBootstrapKeyByte byte = 0x04
	KVEngineMetaKeyByte     byte = 0x05
)

func StoreIdentKey() []byte {
	return []byte{StoreIdentKeyByte}
}

func PrepareBootstrapKey() []byte {
	return []byte{PrepareBootstrapKeyByte}
}

func RaftStateKey(version uint64) []byte {
	key := make([]byte, 5)
	key[0] = RaftStateKeyByte
	binary.BigEndian.PutUint32(key[1:], uint32(version))
	return key
}

func RegionStateKey(version, confVer uint64) []byte {
	key := make([]byte, 9)
	key[0] = RegionMetaKeyByte
	binary.BigEndian.PutUint32(key[1:], uint32(version))
	binary.BigEndian.PutUint32(key[5:], uint32(confVer))
	return key
}

func KVEngineMetaKey() []byte {
	return []byte{KVEngineMetaKeyByte}
}

func ParseRegionStateKey(key []byte) (version, confVer uint64) {
	version = uint64(binary.BigEndian.Uint32(key[1:]))
	confVer = uint64(binary.BigEndian.Uint32(key[5:]))
	return
}

// Get the `start_key` of current region in encoded form.
func RawStartKey(region *metapb.Region) []byte {
	// only initialized region's start_key can be encoded, otherwise there must be bugs
	// somewhere.
	if len(region.StartKey) == 0 {
		// Data starts with 0x01 is used as local key.
		return rawInitialStartKey
	}
	_, decoded, err := codec.DecodeBytes(region.StartKey, nil)
	if err != nil {
		panic(fmt.Sprint(err, region.StartKey))
	}
	return decoded
}

// Get the `end_key` of current region in encoded form.
func RawEndKey(region *metapb.Region) []byte {
	// only initialized region's end_key can be encoded, otherwise there must be bugs
	// somewhere.
	if len(region.EndKey) == 0 {
		return rawInitialEndKey
	}
	_, decoded, err := codec.DecodeBytes(region.EndKey, nil)
	y.Assert(err == nil)
	return decoded
}
