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
	"github.com/pingcap/badger/y"
)

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

const (
	BitDelete byte = 1 << 0 // Set if the key has been deleted.
)

func IsDeleted(meta byte) bool {
	return meta&BitDelete > 0
}
