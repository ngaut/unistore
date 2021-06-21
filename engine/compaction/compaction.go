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

package compaction

import (
	"bytes"
	"fmt"
	"github.com/ngaut/unistore/engine/table"
	"github.com/ngaut/unistore/engine/table/sstable"
	"github.com/ngaut/unistore/s3util"
	"github.com/ngaut/unistore/scheduler"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"sync"
)

type DiscardStats struct {
	numSkips     int64
	skippedBytes int64
}

func (ds *DiscardStats) collect(vs y.ValueStruct) {
	ds.skippedBytes += int64(len(vs.Value) + len(vs.UserMeta))
	ds.numSkips++
}

func (ds *DiscardStats) String() string {
	return fmt.Sprintf("numSkips:%d, skippedBytes:%d", ds.numSkips, ds.skippedBytes)
}

// CompactTables compacts tables in CompactDef and returns the file names.
func CompactTables(cr *Request, discardStats *DiscardStats, s3c *s3util.S3Client) ([]*sstable.BuildResult, error) {
	var buildResults []*sstable.BuildResult
	it, err := buildIterators(cr, s3c)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var lastKey, skipKey []byte
	var builder *sstable.Builder
	var bt *scheduler.BatchTasks
	if s3c != nil {
		bt = scheduler.NewBatchTasks()
	}
	allocID := cr.FirstID
	for it.Valid() {
		y.Assert(allocID <= cr.LastID)
		id := allocID
		allocID++
		if builder == nil {
			builder = sstable.NewTableBuilder(id, cr.getTableBuilderOptions())
		} else {
			builder.Reset(id)
		}
		lastKey = lastKey[:0]
		for ; it.Valid(); table.NextAllVersion(it) {
			vs := it.Value()
			key := it.Key()
			kvSize := int(vs.EncodedSize()) + len(key)
			// See if we need to skip this key.
			if len(skipKey) > 0 {
				if bytes.Equal(key, skipKey) {
					discardStats.collect(vs)
					continue
				} else {
					skipKey = skipKey[:0]
				}
			}
			if !bytes.Equal(key, lastKey) {
				if shouldFinishFile(lastKey, int64(builder.EstimateSize()+kvSize), cr.MaxTableSize) {
					break
				}
				lastKey = append(lastKey[:0], key...)
			}

			// Only consider the versions which are below the minReadTs, otherwise, we might end up discarding the
			// only valid version for a running transaction.
			if cr.CF == LockCF || vs.Version <= cr.SafeTS {
				// key is the latest readable version of this key, so we simply discard all the rest of the versions.
				skipKey = append(skipKey[:0], key...)

				if table.IsDeleted(vs.Meta) {
					// If this key range has overlap with lower levels, then keep the deletion
					// marker with the latest version, discarding the rest. We have set skipKey,
					// so the following key versions would be skipped. Otherwise discard the deletion marker.
					if !cr.Overlap {
						continue
					}
				} else {
					switch filter(cr.SafeTS, cr.CF, key, vs.Value, vs.UserMeta) {
					case DecisionMarkTombstone:
						discardStats.collect(vs)
						if cr.Overlap {
							// There may have ole versions for this key, so convert to delete tombstone.
							builder.Add(key, &y.ValueStruct{Meta: table.BitDelete, Version: vs.Version})
						}
						continue
					case DecisionDrop:
						discardStats.collect(vs)
						continue
					case DecisionKeep:
					}
				}
			}
			builder.Add(key, &vs)
		}
		if builder.Empty() {
			continue
		}
		w := bytes.NewBuffer(make([]byte, 0, builder.EstimateSize()))
		result, err := builder.Finish(id, w)
		if err != nil {
			return nil, err
		}
		if s3c != nil {
			bt.AppendTask(func() error {
				return s3c.Put(s3c.BlockKey(id), result.FileData)
			})
		}
		buildResults = append(buildResults, result)
	}
	if s3c != nil {
		if err := s3c.BatchSchedule(bt); err != nil {
			return nil, err
		}
	}
	return buildResults, nil
}

func buildIterators(req *Request, s3c *s3util.S3Client) (table.Iterator, error) {
	topFiles, err := loadTableFiles(req.Tops, s3c)
	if err != nil {
		return nil, err
	}
	botFiles, err := loadTableFiles(req.Bottoms, s3c)
	if err != nil {
		return nil, err
	}
	iters := []table.Iterator{
		table.NewConcatIterator(inMemFilesToTables(topFiles), false),
		table.NewConcatIterator(inMemFilesToTables(botFiles), false),
	}
	// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
	it := table.NewMergeIterator(iters, false)
	it.Rewind()
	return it, nil
}

func shouldFinishFile(lastKey []byte, currentSize, maxSize int64) bool {
	return len(lastKey) > 0 && currentSize > maxSize
}

const (
	WriteCF = 0
	LockCF  = 1
	ExtraCF = 2
)

// Decision is the type for compaction filter decision.
type Decision int

const (
	// DecisionKeep indicates the entry should be reserved.
	DecisionKeep Decision = 0
	// DecisionMarkTombstone converts the entry to a delete tombstone.
	DecisionMarkTombstone Decision = 1
	// DecisionDrop simply drops the entry, doesn't leave a delete tombstone.
	DecisionDrop Decision = 2
)

// filter implements the badger.CompactionFilter interface.
// Since we use txn ts as badger version, we only need to filter Delete, Rollback and Op_Lock.
// It is called for the first valid version before safe point, older versions are discarded automatically.
func filter(safePoint uint64, cf int, key, value, userMeta []byte) Decision {
	switch cf {
	case WriteCF:
		if mvcc.UserMeta(userMeta).CommitTS() < safePoint && len(value) == 0 {
			return DecisionMarkTombstone
		}
	case LockCF:
		return DecisionKeep
	case ExtraCF:
		if mvcc.UserMeta(userMeta).StartTS() < safePoint {
			return DecisionDrop
		}
	}
	// Older version are discarded automatically, we need to keep the first valid version.
	return DecisionKeep
}

type compactL0Helper struct {
	cf      int
	builder *sstable.Builder
	lastKey []byte
	skipKey []byte
	iter    table.Iterator
	cReq    *Request
}

func newCompactL0Helper(topTables []*sstable.L0Table, botTables []table.Table, cf int, cReq *Request) *compactL0Helper {
	helper := &compactL0Helper{cf: cf, cReq: cReq}
	var iters []table.Iterator
	if topTables != nil {
		for _, tbl := range topTables {
			it := tbl.NewIterator(cf, false)
			if it != nil {
				iters = append(iters, it)
			}
		}
	}
	if len(botTables) > 0 {
		iters = append(iters, table.NewConcatIterator(botTables, false))
	}
	helper.iter = table.NewMergeIterator(iters, false)
	helper.iter.Rewind()
	return helper
}

func (h *compactL0Helper) setFID(fid uint64) {
	if h.builder == nil {
		h.builder = sstable.NewTableBuilder(fid, h.cReq.getTableBuilderOptions())
	} else {
		h.builder.Reset(fid)
	}
}

func (h *compactL0Helper) buildOne(id uint64) (*sstable.BuildResult, error) {
	h.setFID(id)
	h.lastKey = h.lastKey[:0]
	h.skipKey = h.skipKey[:0]
	it := h.iter
	for ; it.Valid(); table.NextAllVersion(it) {
		vs := it.Value()
		key := it.Key()
		// See if we need to skip this key.
		if len(h.skipKey) > 0 {
			if bytes.Equal(key, h.skipKey) {
				continue
			} else {
				h.skipKey = h.skipKey[:0]
			}
		}
		if !bytes.Equal(key, h.lastKey) {
			// We only break on table size.
			if h.builder.EstimateSize() > int(h.cReq.MaxTableSize) {
				break
			}
			h.lastKey = append(h.lastKey[:0], key...)
		}

		// Only consider the versions which are below the safeTS, otherwise, we might end up discarding the
		// only valid version for a running transaction.
		if vs.Version <= h.cReq.SafeTS {
			// key is the latest readable version of this key, so we simply discard all the rest of the versions.
			h.skipKey = append(h.skipKey[:0], key...)
			if !table.IsDeleted(vs.Meta) {
				switch filter(h.cReq.SafeTS, h.cf, key, vs.Value, vs.UserMeta) {
				case DecisionMarkTombstone:
					// There may have ole versions for this key, so convert to delete tombstone.
					h.builder.Add(key, &y.ValueStruct{Meta: table.BitDelete, Version: vs.Version})
					continue
				case DecisionDrop:
					continue
				case DecisionKeep:
				}
			}
		}
		h.builder.Add(key, &vs)
	}
	if h.builder.Empty() {
		return nil, nil
	}
	result, err := h.builder.Finish(id, bytes.NewBuffer(make([]byte, 0, h.builder.EstimateSize())))
	if err != nil {
		return nil, err
	}
	return result, nil
}

func compactL0(req *Request, s3c *s3util.S3Client) ([][]*sstable.BuildResult, error) {
	l0Files, err := loadTableFiles(req.Tops, s3c)
	if err != nil {
		return nil, err
	}
	allResults := make([][]*sstable.BuildResult, len(req.MultiCFBottoms))
	l0Tbls := inMemFilesToL0Tables(l0Files)
	bt := scheduler.NewBatchTasks()
	allocID := req.FirstID
	for cf := 0; cf < len(req.MultiCFBottoms); cf++ {
		botIDs := req.MultiCFBottoms[cf]
		botFiles, err1 := loadTableFiles(botIDs, s3c)
		if err1 != nil {
			return nil, err1
		}
		helper := newCompactL0Helper(l0Tbls, inMemFilesToTables(botFiles), cf, req)
		defer helper.iter.Close()
		var results []*sstable.BuildResult
		for {
			y.Assert(allocID <= req.LastID)
			id := allocID
			allocID++
			result, err2 := helper.buildOne(id)
			if err2 != nil {
				return nil, err2
			}
			if result == nil {
				break
			}
			bt.AppendTask(func() error {
				return s3c.Put(s3c.BlockKey(id), result.FileData)
			})
			results = append(results, result)
		}
		allResults[cf] = results
	}
	err = s3c.BatchSchedule(bt)
	if err != nil {
		return nil, err
	}
	return allResults, nil
}

func loadTableFiles(tableIDs []uint64, s3c *s3util.S3Client) ([]*sstable.InMemFile, error) {
	m := sync.Map{}
	tasks := scheduler.NewBatchTasks()
	for i := range tableIDs {
		tableID := tableIDs[i]
		tasks.AppendTask(func() error {
			log.S().Infof("get object %s", s3c.BlockKey(tableID))
			fileData, err := s3c.Get(s3c.BlockKey(tableID), 0, 0)
			if err != nil {
				return err
			}
			y.Assert(len(fileData) > 0)
			tblFile := sstable.NewInMemFile(tableID, fileData)
			m.Store(tableID, tblFile)
			return nil
		})
	}
	err := s3c.BatchSchedule(tasks)
	if err != nil {
		return nil, err
	}
	files := make([]*sstable.InMemFile, len(tableIDs))
	for i, tblID := range tableIDs {
		tf, ok := m.Load(tblID)
		y.Assert(ok)
		files[i] = tf.(*sstable.InMemFile)
	}
	return files, nil
}

func inMemFilesToTables(files []*sstable.InMemFile) []table.Table {
	tables := make([]table.Table, len(files))
	for i, file := range files {
		tables[i], _ = sstable.OpenTable(file, nil)
	}
	return tables
}

func inMemFilesToL0Tables(files []*sstable.InMemFile) []*sstable.L0Table {
	tables := make([]*sstable.L0Table, len(files))
	for i, file := range files {
		tables[i], _ = sstable.OpenL0Table(file)
	}
	return tables
}
