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

package tikv

import (
	"fmt"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

// ErrLocked is returned when trying to Read/Write on a locked key. Client should
// backoff or cleanup the lock then retry.
type ErrLocked struct {
	Key         []byte
	Primary     []byte
	StartTS     uint64
	TTL         uint64
	LockType    uint8
	minCommitTS uint64
}

// BuildLockErr generates ErrKeyLocked objects
func BuildLockErr(key []byte, primaryKey []byte, startTS uint64, TTL uint64, lockType uint8, minCommitTS uint64) *ErrLocked {
	errLocked := &ErrLocked{
		Key:         key,
		Primary:     primaryKey,
		StartTS:     startTS,
		TTL:         TTL,
		LockType:    lockType,
		minCommitTS: minCommitTS,
	}
	return errLocked
}

// Error formats the lock to a string.
func (e *ErrLocked) Error() string {
	return fmt.Sprintf("key is locked, key: %q, Type: %v, primary: %q, startTS: %v", e.Key, e.LockType, e.Primary, e.StartTS)
}

// ErrRetryable suggests that client may restart the txn. e.g. write conflict.
type ErrRetryable string

func (e ErrRetryable) Error() string {
	return fmt.Sprintf("retryable: %s", string(e))
}

var (
	ErrLockNotFound    = ErrRetryable("lock not found")
	ErrAlreadyRollback = ErrRetryable("already rollback")
	ErrReplaced        = ErrRetryable("replaced by another transaction")
)

type ErrInvalidOp struct {
	op kvrpcpb.Op
}

func (e ErrInvalidOp) Error() string {
	return fmt.Sprintf("invalid op: %s", e.op.String())
}

// ErrAlreadyCommitted is returned specially when client tries to rollback a
// committed lock.
type ErrAlreadyCommitted uint64

func (e ErrAlreadyCommitted) Error() string {
	return "txn already committed"
}

type ErrKeyAlreadyExists struct {
	Key []byte
}

func (e ErrKeyAlreadyExists) Error() string {
	return "key already exists"
}

// ErrDeadlock is returned when deadlock is detected.
type ErrDeadlock struct {
	LockKey         []byte
	LockTS          uint64
	DeadlockKeyHash uint64
}

func (e ErrDeadlock) Error() string {
	return "deadlock"
}

type ErrConflict struct {
	StartTS          uint64
	ConflictTS       uint64
	ConflictCommitTS uint64
	Key              []byte
}

func (e *ErrConflict) Error() string {
	return "write conflict"
}

// ErrCommitExpire is returned when commit key commitTs smaller than lock.MinCommitTs
type ErrCommitExpire struct {
	StartTs     uint64
	CommitTs    uint64
	MinCommitTs uint64
	Key         []byte
}

func (e *ErrCommitExpire) Error() string {
	return "commit expired"
}

// ErrTxnNotFound is returned if the required txn info not found on storage
type ErrTxnNotFound struct {
	StartTS    uint64
	PrimaryKey []byte
}

func (e *ErrTxnNotFound) Error() string {
	return "txn not found"
}
