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

package raftlog

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
)

type CustomRaftLogType byte

const (
	CustomRaftLogFlag byte = 64

	TypePrewrite            CustomRaftLogType = 1
	TypeCommit              CustomRaftLogType = 2
	TypeRolback             CustomRaftLogType = 3
	TypePessimisticLock     CustomRaftLogType = 4
	TypePessimisticRollback CustomRaftLogType = 5
)

// CustomRaftLog is the raft log format for unistore to store Prewrite/Commit/PessimisticLock.
//  | flag(1) | type(1) | version(2) | header(40) | entries
//
// It reduces the cost of marshal/unmarshal and avoid DB lookup during apply.
type CustomRaftLog struct {
	header *CustomHeader
	Data   []byte
}

func NewCustom(data []byte) *CustomRaftLog {
	rlog := &CustomRaftLog{}
	rlog.header = (*CustomHeader)(unsafe.Pointer(&data[4]))
	rlog.Data = data
	return rlog
}

func (c *CustomRaftLog) Type() CustomRaftLogType {
	return CustomRaftLogType(c.Data[1])
}

func (c *CustomRaftLog) RegionID() uint64 {
	return c.header.RegionID
}

func (c *CustomRaftLog) PeerID() uint64 {
	return c.header.PeerID
}

func (c *CustomRaftLog) StoreID() uint64 {
	return c.header.StoreID
}

func (c *CustomRaftLog) Epoch() Epoch {
	return c.header.Epoch
}

func (c *CustomRaftLog) Term() uint64 {
	return c.header.Term
}

func (c *CustomRaftLog) Marshal() []byte {
	return c.Data
}

func (c *CustomRaftLog) GetRaftCmdRequest() *raft_cmdpb.RaftCmdRequest {
	return nil
}

type CustomHeader struct {
	RegionID uint64
	Epoch    Epoch
	PeerID   uint64
	StoreID  uint64
	Term     uint64
}

const headerSize = int(unsafe.Sizeof(CustomHeader{}))

func (h *CustomHeader) Marshal() []byte {
	data := make([]byte, headerSize)
	*(*CustomHeader)(unsafe.Pointer(&data[0])) = *h
	return data
}

type Epoch struct {
	ver     uint32
	confVer uint32
}

func NewEpoch(ver, confVer uint64) Epoch {
	return Epoch{ver: uint32(ver), confVer: uint32(confVer)}
}

func (e Epoch) Ver() uint64 {
	return uint64(e.ver)
}

func (e Epoch) ConfVer() uint64 {
	return uint64(e.confVer)
}

func (e Epoch) String() string {
	return fmt.Sprintf("{Ver:%d, ConfVer:%d}", e.ver, e.confVer)
}

func (rl *CustomRaftLog) IterateLock(itFunc func(key, val []byte)) {
	i := 4 + headerSize
	for i < len(rl.Data) {
		keyLen := endian.Uint16(rl.Data[i:])
		i += 2
		key := rl.Data[i : i+int(keyLen)]
		i += int(keyLen)
		valLen := endian.Uint32(rl.Data[i:])
		i += 4
		val := rl.Data[i : i+int(valLen)]
		i += int(valLen)
		itFunc(key, val)
	}
}

func (rl *CustomRaftLog) IterateCommit(itFunc func(key, val []byte, commitTS uint64)) {
	i := 4 + headerSize
	for i < len(rl.Data) {
		keyLen := endian.Uint16(rl.Data[i:])
		i += 2
		key := rl.Data[i : i+int(keyLen)]
		i += int(keyLen)
		valLen := endian.Uint32(rl.Data[i:])
		i += 4
		val := rl.Data[i : i+int(valLen)]
		i += int(valLen)
		commitTS := endian.Uint64(rl.Data[i:])
		i += 8
		itFunc(key, val, commitTS)
	}
}

func (rl *CustomRaftLog) IterateRollback(itFunc func(key []byte, startTS uint64, deleteLock bool)) {
	i := 4 + headerSize
	for i < len(rl.Data) {
		keyLen := endian.Uint16(rl.Data[i:])
		i += 2
		key := rl.Data[i : i+int(keyLen)]
		i += int(keyLen)
		startTS := endian.Uint64(rl.Data[i:])
		i += 8
		del := rl.Data[i]
		i++
		itFunc(key, startTS, del > 0)
	}
}

func (rl *CustomRaftLog) IteratePessimisticRollback(itFunc func(key []byte)) {
	i := 4 + headerSize
	for i < len(rl.Data) {
		keyLen := endian.Uint16(rl.Data[i:])
		i += 2
		key := rl.Data[i : i+int(keyLen)]
		i += int(keyLen)
		itFunc(key)
	}
}

type CustomBuilder struct {
	data []byte
	cnt  int
}

func NewBuilder(header CustomHeader) *CustomBuilder {
	b := &CustomBuilder{}
	b.data = append(b.data, CustomRaftLogFlag, 0, 0, 0)
	b.data = append(b.data, header.Marshal()...)
	return b
}

func (b *CustomBuilder) AppendLock(key, value []byte) {
	b.data = append(b.data, u16ToBytes(uint16(len(key)))...)
	b.data = append(b.data, key...)
	b.data = append(b.data, u32ToBytes(uint32(len(value)))...)
	b.data = append(b.data, value...)
	b.cnt++
}

func (b *CustomBuilder) AppendCommit(key, value []byte, commitTS uint64) {
	b.data = append(b.data, u16ToBytes(uint16(len(key)))...)
	b.data = append(b.data, key...)
	b.data = append(b.data, u32ToBytes(uint32(len(value)))...)
	b.data = append(b.data, value...)
	b.data = append(b.data, u64ToBytes(commitTS)...)
	b.cnt++
}

func (b *CustomBuilder) AppendRollback(key []byte, startTS uint64, deleteLock bool) {
	b.data = append(b.data, u16ToBytes(uint16(len(key)))...)
	b.data = append(b.data, key...)
	b.data = append(b.data, u64ToBytes(startTS)...)
	if deleteLock {
		b.data = append(b.data, 1)
	} else {
		b.data = append(b.data, 0)
	}
	b.cnt++
}

func (b *CustomBuilder) AppendPessimisticRollback(key []byte) {
	b.data = append(b.data, u16ToBytes(uint16(len(key)))...)
	b.data = append(b.data, key...)
	b.cnt++
}

func (b *CustomBuilder) SetType(tp CustomRaftLogType) {
	b.data[1] = byte(tp)
}

func (b *CustomBuilder) GetType() CustomRaftLogType {
	return CustomRaftLogType(b.data[1])
}

func (b *CustomBuilder) Build() *CustomRaftLog {
	return NewCustom(b.data)
}

func (b *CustomBuilder) Len() int {
	return b.cnt
}

var endian = binary.LittleEndian

func u16ToBytes(v uint16) []byte {
	b := make([]byte, 2)
	endian.PutUint16(b, v)
	return b
}

func u32ToBytes(v uint32) []byte {
	b := make([]byte, 4)
	endian.PutUint32(b, v)
	return b
}

func u64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	endian.PutUint64(b, v)
	return b
}
