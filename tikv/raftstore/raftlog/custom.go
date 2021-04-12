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

// CustomRaftLogType describes the type of a CustomRaftLog.
type CustomRaftLogType byte

// CustomRaftLogType
const (
	// CustomRaftLogFlag is a CustomRaftLog flag.
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

// NewCustom returns a new CustomRaftLog.
func NewCustom(data []byte) *CustomRaftLog {
	rlog := &CustomRaftLog{}
	rlog.header = (*CustomHeader)(unsafe.Pointer(&data[4]))
	rlog.Data = data
	return rlog
}

// Type returns the type of the CustomRaftLog.
func (c *CustomRaftLog) Type() CustomRaftLogType {
	return CustomRaftLogType(c.Data[1])
}

// RegionID implements the RaftLog RegionID method.
func (c *CustomRaftLog) RegionID() uint64 {
	return c.header.RegionID
}

// PeerID implements the RaftLog PeerID method.
func (c *CustomRaftLog) PeerID() uint64 {
	return c.header.PeerID
}

// StoreID implements the RaftLog StoreID method.
func (c *CustomRaftLog) StoreID() uint64 {
	return c.header.StoreID
}

// Epoch implements the RaftLog Epoch method.
func (c *CustomRaftLog) Epoch() Epoch {
	return c.header.Epoch
}

// Term implements the RaftLog Term method.
func (c *CustomRaftLog) Term() uint64 {
	return c.header.Term
}

// Marshal implements the RaftLog Marshal method.
func (c *CustomRaftLog) Marshal() []byte {
	return c.Data
}

// GetRaftCmdRequest implements the RaftLog GetRaftCmdRequest method.
func (c *CustomRaftLog) GetRaftCmdRequest() *raft_cmdpb.RaftCmdRequest {
	return nil
}

// CustomHeader represents a custom header.
type CustomHeader struct {
	RegionID uint64
	Epoch    Epoch
	PeerID   uint64
	StoreID  uint64
	Term     uint64
}

const headerSize = int(unsafe.Sizeof(CustomHeader{}))

// Marshal returns the encoded bytes.
func (h *CustomHeader) Marshal() []byte {
	data := make([]byte, headerSize)
	*(*CustomHeader)(unsafe.Pointer(&data[0])) = *h
	return data
}

// Epoch stores the information about its epoch.
type Epoch struct {
	ver     uint32
	confVer uint32
}

// NewEpoch returns a new Epoch with the given ver and confVer.
func NewEpoch(ver, confVer uint64) Epoch {
	return Epoch{ver: uint32(ver), confVer: uint32(confVer)}
}

// Ver returns the version of the Epoch.
func (e Epoch) Ver() uint64 {
	return uint64(e.ver)
}

// ConfVer returns the config change version of the Epoch.
func (e Epoch) ConfVer() uint64 {
	return uint64(e.confVer)
}

// String implements fmt.Stringer interface.
func (e Epoch) String() string {
	return fmt.Sprintf("{Ver:%d, ConfVer:%d}", e.ver, e.confVer)
}

// IterateLock iterates through all locks of the CustomRaftLog.
func (c *CustomRaftLog) IterateLock(itFunc func(key, val []byte)) {
	i := 4 + headerSize
	for i < len(c.Data) {
		keyLen := endian.Uint16(c.Data[i:])
		i += 2
		key := c.Data[i : i+int(keyLen)]
		i += int(keyLen)
		valLen := endian.Uint32(c.Data[i:])
		i += 4
		val := c.Data[i : i+int(valLen)]
		i += int(valLen)
		itFunc(key, val)
	}
}

// IterateCommit iterates through all commits of the CustomRaftLog.
func (c *CustomRaftLog) IterateCommit(itFunc func(key, val []byte, commitTS uint64)) {
	i := 4 + headerSize
	for i < len(c.Data) {
		keyLen := endian.Uint16(c.Data[i:])
		i += 2
		key := c.Data[i : i+int(keyLen)]
		i += int(keyLen)
		valLen := endian.Uint32(c.Data[i:])
		i += 4
		val := c.Data[i : i+int(valLen)]
		i += int(valLen)
		commitTS := endian.Uint64(c.Data[i:])
		i += 8
		itFunc(key, val, commitTS)
	}
}

// IterateRollback iterates through all rollbacks of the CustomRaftLog.
func (c *CustomRaftLog) IterateRollback(itFunc func(key []byte, startTS uint64, deleteLock bool)) {
	i := 4 + headerSize
	for i < len(c.Data) {
		keyLen := endian.Uint16(c.Data[i:])
		i += 2
		key := c.Data[i : i+int(keyLen)]
		i += int(keyLen)
		startTS := endian.Uint64(c.Data[i:])
		i += 8
		del := c.Data[i]
		i++
		itFunc(key, startTS, del > 0)
	}
}

// IteratePessimisticRollback iterates through all pessimistic rollbacks of the CustomRaftLog.
func (c *CustomRaftLog) IteratePessimisticRollback(itFunc func(key []byte)) {
	i := 4 + headerSize
	for i < len(c.Data) {
		keyLen := endian.Uint16(c.Data[i:])
		i += 2
		key := c.Data[i : i+int(keyLen)]
		i += int(keyLen)
		itFunc(key)
	}
}

// CustomBuilder represents a custom builder.
type CustomBuilder struct {
	data []byte
	cnt  int
}

// NewBuilder returns a new CustomBuilder.
func NewBuilder(header CustomHeader) *CustomBuilder {
	b := &CustomBuilder{}
	b.data = append(b.data, CustomRaftLogFlag, 0, 0, 0)
	b.data = append(b.data, header.Marshal()...)
	return b
}

// AppendLock appends a lock into the CustomBuilder.
func (b *CustomBuilder) AppendLock(key, value []byte) {
	b.data = append(b.data, u16ToBytes(uint16(len(key)))...)
	b.data = append(b.data, key...)
	b.data = append(b.data, u32ToBytes(uint32(len(value)))...)
	b.data = append(b.data, value...)
	b.cnt++
}

// AppendCommit appends a commit into the CustomBuilder.
func (b *CustomBuilder) AppendCommit(key, value []byte, commitTS uint64) {
	b.data = append(b.data, u16ToBytes(uint16(len(key)))...)
	b.data = append(b.data, key...)
	b.data = append(b.data, u32ToBytes(uint32(len(value)))...)
	b.data = append(b.data, value...)
	b.data = append(b.data, u64ToBytes(commitTS)...)
	b.cnt++
}

// AppendRollback appends a rollback into the CustomBuilder.
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

// AppendPessimisticRollback appends a pessimistic rollback into the CustomBuilder.
func (b *CustomBuilder) AppendPessimisticRollback(key []byte) {
	b.data = append(b.data, u16ToBytes(uint16(len(key)))...)
	b.data = append(b.data, key...)
	b.cnt++
}

// SetType sets the CustomRaftLogType of the CustomBuilder.
func (b *CustomBuilder) SetType(tp CustomRaftLogType) {
	b.data[1] = byte(tp)
}

// GetType gets the CustomRaftLogType of the CustomBuilder.
func (b *CustomBuilder) GetType() CustomRaftLogType {
	return CustomRaftLogType(b.data[1])
}

// Build creates a new CustomRaftLog.
func (b *CustomBuilder) Build() *CustomRaftLog {
	return NewCustom(b.data)
}

// Len gets the length of the CustomBuilder.
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
