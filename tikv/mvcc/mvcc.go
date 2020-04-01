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

package mvcc

import (
	"encoding/binary"
	"unsafe"

	"github.com/pingcap/tidb/util/codec"
)

var defaultEndian = binary.LittleEndian

// DBUserMeta is the user meta used in DB.
type DBUserMeta []byte

const dbUserMetaLen = 16

// DecodeLock decodes data to lock, the primary and value is copied.
func DecodeLock(data []byte) (l MvccLock) {
	l.MvccLockHdr = *(*MvccLockHdr)(unsafe.Pointer(&data[0]))
	cursor := mvccLockHdrSize
	lockBuf := append([]byte{}, data[cursor:]...)
	l.Primary = lockBuf[:l.PrimaryLen]
	l.Value = lockBuf[l.PrimaryLen:]
	return
}

// MvccLockHdr holds fixed size fields for MvccLock.
type MvccLockHdr struct {
	StartTS     uint64
	ForUpdateTS uint64
	MinCommitTS uint64
	TTL         uint32
	Op          uint8
	HasOldVer   bool
	PrimaryLen  uint16
}

const mvccLockHdrSize = int(unsafe.Sizeof(MvccLockHdr{}))

// MvccLock is the structure for MVCC lock.
type MvccLock struct {
	MvccLockHdr
	Primary []byte
	Value   []byte
}

// MarshalBinary implements encoding.BinaryMarshaler interface.
func (l *MvccLock) MarshalBinary() []byte {
	lockLen := mvccLockHdrSize + len(l.Primary) + len(l.Value)
	length := lockLen
	buf := make([]byte, length)
	hdr := (*MvccLockHdr)(unsafe.Pointer(&buf[0]))
	*hdr = l.MvccLockHdr
	cursor := mvccLockHdrSize
	copy(buf[cursor:], l.Primary)
	cursor += len(l.Primary)
	copy(buf[cursor:], l.Value)
	cursor += len(l.Value)
	return buf
}

// UserMeta value for lock.
const (
	LockUserMetaNoneByte   = 0
	LockUserMetaDeleteByte = 2
)

// UserMeta byte slices for lock.
var (
	LockUserMetaNone   = []byte{LockUserMetaNoneByte}
	LockUserMetaDelete = []byte{LockUserMetaDeleteByte}
)

// DecodeKeyTS decodes the TS in a key.
func DecodeKeyTS(buf []byte) uint64 {
	tsBin := buf[len(buf)-8:]
	_, ts, err := codec.DecodeUintDesc(tsBin)
	if err != nil {
		panic(err)
	}
	return ts
}

// NewDBUserMeta creates a new DBUserMeta.
func NewDBUserMeta(startTS, commitTS uint64) DBUserMeta {
	m := make(DBUserMeta, 16)
	defaultEndian.PutUint64(m, startTS)
	defaultEndian.PutUint64(m[8:], commitTS)
	return m
}

// CommitTS reads the commitTS from the DBUserMeta.
func (m DBUserMeta) CommitTS() uint64 {
	return defaultEndian.Uint64(m[8:])
}

// StartTS reads the startTS from the DBUserMeta.
func (m DBUserMeta) StartTS() uint64 {
	return defaultEndian.Uint64(m[:8])
}

// EncodeExtraTxnStatusKey encodes a extra transaction status key.
// It is only used for Rollback and Op_Lock.
func EncodeExtraTxnStatusKey(key []byte, startTS uint64) []byte {
	b := append([]byte{}, key...)
	ret := codec.EncodeUintDesc(b, startTS)
	ret[0]++
	return ret
}

// DecodeExtraTxnStatusKey decodes a extra transaction status key.
func DecodeExtraTxnStatusKey(extraKey []byte) (key []byte) {
	if len(extraKey) <= 9 {
		return nil
	}
	key = append([]byte{}, extraKey[:len(extraKey)-8]...)
	key[0]--
	return
}
