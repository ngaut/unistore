// Copyright 2021-present PingCAP, Inc.
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

package table

import (
	"github.com/ngaut/unistore/sdb/epoch"
	"github.com/pingcap/badger/y"
)

type Table interface {
	epoch.Resource
	ID() uint64
	NewIterator(reversed bool) Iterator
	Get(key []byte, version, keyHash uint64) (y.ValueStruct, error)
	Size() int64
	Smallest() []byte
	Biggest() []byte
	HasOverlap(start, end []byte, includeEnd bool) bool
	Close() error
}

type Iterator interface {
	// Next returns the next entry with different key on the latest version.
	// If old version is needed, call NextVersion.
	Next()
	// NextVersion set the current entry to an older version.
	// The iterator must be valid to call this method.
	// It returns true if there is an older version, returns false if there is no older version.
	// The iterator is still valid and on the same key.
	NextVersion() bool
	Rewind()
	Seek(key []byte)
	Key() []byte
	Value() y.ValueStruct
	FillValue(vs *y.ValueStruct)
	Valid() bool
	Close() error
}

// SeekToVersion seeks a valid Iterator to the version that <= the given version.
func SeekToVersion(it Iterator, version uint64) bool {
	if version >= it.Value().Version {
		return true
	}
	for it.NextVersion() {
		if version >= it.Value().Version {
			return true
		}
	}
	return false
}

func NextAllVersion(it Iterator) {
	if !it.NextVersion() {
		it.Next()
	}
}
