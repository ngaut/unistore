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

package dfs

import "github.com/ngaut/unistore/scheduler"

// DFS represents a distributed file system.
type DFS interface {
	// Open opens an existing file with fileID.
	// It may take a long time if the file need to be cached in local disk.
	Open(fileID uint64, opts Options) (File, error)

	// Prefetch fetches the data from remote server to local disk cache for lower read latency.
	Prefetch(fileID uint64, opts Options) error

	// ReadFile reads the whole file to memory.
	// It can be used by remote compaction server that doesn't have local disk.
	ReadFile(fileID uint64, opts Options) ([]byte, error)

	// Create creates a new File.
	// The regionID and regionVer can be used determine where to write the file.
	Create(fileID uint64, data []byte, opts Options) error

	// Remove removes the file from the DFS.
	Remove(fileID uint64, opts Options) error

	// GetScheduler gets the scheduler for this DFS.
	GetScheduler() *scheduler.Scheduler
}

// Options provides additional information that may be used by the DFS.
type Options struct {
	ShardID  uint64
	ShardVer uint64
}

// NewOptions creates a new Options.
func NewOptions(shardID, shardVer uint64) Options {
	return Options{ShardID: shardID, ShardVer: shardVer}
}

// File represents a file in the DFS.
type File interface {
	// ID returns the id of the file.
	ID() uint64

	// Size returns the size of the file.
	Size() int64

	// ReadAt reads the data.
	ReadAt(b []byte, off int64) (n int, err error)

	// InMem checks if the file is in memory.
	InMem() bool

	// MemSlice returns the slice of memory data, should only be called if InMem is true.
	MemSlice(off, length int) []byte

	// Close releases the local resource like on-disk cache or memory of the file.
	// It should be called when a file will not be used locally but may be used by other nodes.
	// For example when a peer is moved out of the store.
	// Should not be called on process exit.
	Close() error
}
