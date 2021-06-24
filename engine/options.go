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

/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package engine

import (
	"github.com/ngaut/unistore/engine/table/sstable"
	"github.com/ngaut/unistore/enginepb"
	"github.com/ngaut/unistore/s3util"
)

// NOTE: Keep the comments in the following to 75 chars width, so they
// format nicely in godoc.

// Options are params for creating Engine object.
//
// This package provides DefaultOptions which contains options that should
// work for most applications. Consider using that as a starting point before
// customizing it for your own needs.
type Options struct {
	// 1. Mandatory flags
	// -------------------
	// Directory to store the data in. Should exist and be writable.
	Dir string

	// BaseSize is th maximum L1 size before trigger a compaction.
	// The L2 size is 10x of the base size, L3 size is 100x of the base size.
	BaseSize int64
	// Maximum number of tables to keep in memory, before stalling.
	NumMemtables int

	// If we hit this number of Level 0 tables, we will stall until L0 is
	// compacted away.
	NumLevelZeroTablesStall int

	MaxBlockCacheSize int64

	// Number of compaction workers to run concurrently.
	NumCompactors int

	// 4. Flags for testing purposes
	// ------------------------------
	DoNotCompact bool // Stops LSM tree from compactions.

	TableBuilderOptions sstable.TableBuilderOptions

	RemoteCompactionAddr string

	InstanceID uint32

	S3Options s3util.Options

	CFs []CFConfig

	IDAllocator IDAllocator

	MetaChangeListener MetaChangeListener

	RecoverHandler RecoverHandler

	// Max mem size is dynamically adjusted for each time the mem-table get flushed.
	// The formula is (factor * write_bytes_per_second)
	// And limited in range [2MB, 256MB].
	MaxMemTableSizeFactor int
}

type CFConfig struct {
	Managed   bool
	MaxLevels int
}

type IDAllocator interface {
	AllocID(count int) (lastID uint64, err error)
}

// MetaChangeListener is used to notify the engine user that engine meta has changed.
type MetaChangeListener interface {
	OnChange(e *enginepb.ChangeSet)
}

var DefaultOpt = Options{
	DoNotCompact:            false,
	BaseSize:                64 << 20,
	NumCompactors:           3,
	NumLevelZeroTablesStall: 10,
	NumMemtables:            16,
	TableBuilderOptions: sstable.TableBuilderOptions{
		LevelSizeMultiplier: 10,
		MaxTableSize:        8 << 20,
		WriteBufferSize:     2 * 1024 * 1024,
		BytesPerSecond:      -1,
		BlockSize:           64 * 1024,
		LogicalBloomFPR:     0.01,
		MaxLevels:           5,
	},
	CFs: []CFConfig{
		{Managed: true, MaxLevels: 3},
		{Managed: false, MaxLevels: 2},
		{Managed: true, MaxLevels: 1},
	},
	MaxMemTableSizeFactor: 256,
}