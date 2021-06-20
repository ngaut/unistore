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
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

// EngineWriter is the interface to persistent data.
type EngineWriter interface {
	Open()
	Close()
	Write(batch WriteBatch) error
	DeleteRange(start, end []byte, latchHandle LatchHandle) error
	NewWriteBatch(startTS, commitTS uint64, ctx *kvrpcpb.Context) WriteBatch
}

type LatchHandle interface {
	AcquireLatches(hashVals []uint64)
	ReleaseLatches(hashVals []uint64)
}

type WriteBatch interface {
	Prewrite(key []byte, lock *MvccLock)
	Commit(key []byte, lock *MvccLock)
	Rollback(key []byte, deleleLock bool)
	PessimisticLock(key []byte, lock *MvccLock)
	PessimisticRollback(key []byte)
}
