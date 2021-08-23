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

package engine

import (
	"bytes"
	"github.com/ngaut/unistore/engine/dfs"
	"github.com/ngaut/unistore/engine/table/memtable"
	"github.com/ngaut/unistore/engine/table/sstable"
	"github.com/ngaut/unistore/enginepb"
	"github.com/pingcap/badger/y"
	"sort"
	"sync/atomic"
	"unsafe"
)

type IngestTree struct {
	ChangeSet *enginepb.ChangeSet
	Passive   bool
}

func (en *Engine) Ingest(ingestTree *IngestTree) error {
	guard := en.resourceMgr.Acquire()
	defer guard.Done()
	l0s, levelHandlers, err := en.createIngestTreeLevelHandlers(ingestTree)
	if err != nil {
		return err
	}

	shard := newShardForIngest(ingestTree.ChangeSet, &en.opt)
	shard.SetPassive(ingestTree.Passive)
	atomic.StorePointer(shard.memTbls, unsafe.Pointer(&memTables{tables: []*memtable.Table{memtable.NewCFTable(en.numCFs)}}))
	atomic.StorePointer(shard.l0s, unsafe.Pointer(l0s))
	shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		scf := shard.cfs[cf]
		y.Assert(scf.casLevelHandler(level.level, level, levelHandlers[cf][level.level-1]))
		return false
	})
	oldVal, ok := en.shardMap.Load(shard.ID)
	if ok {
		oldShard := oldVal.(*Shard)
		allFiles := en.getAllFiles(ingestTree)
		en.removeShardFiles(oldShard, func(id uint64) bool {
			_, contains := allFiles[id]
			return !contains
		})
	}
	en.shardMap.Store(shard.ID, shard)
	return nil
}

func (en *Engine) getAllFiles(tree *IngestTree) map[uint64]struct{} {
	snap := tree.ChangeSet.Snapshot
	files := map[uint64]struct{}{}
	for _, l0 := range snap.L0Creates {
		files[l0.ID] = struct{}{}
	}
	for _, t := range snap.TableCreates {
		files[t.ID] = struct{}{}
	}
	return files
}

func (en *Engine) createIngestTreeLevelHandlers(ingestTree *IngestTree) (*l0Tables, [][]*levelHandler, error) {
	l0s := &l0Tables{}
	newHandlers := make([][]*levelHandler, en.numCFs)
	for cf := 0; cf < en.numCFs; cf++ {
		for l := 1; l <= en.getCFMaxLevel(cf); l++ {
			newHandler := newLevelHandler(en.opt.NumLevelZeroTablesStall, l)
			newHandlers[cf] = append(newHandlers[cf], newHandler)
		}
	}
	fsOpts := dfs.NewOptions(ingestTree.ChangeSet.ShardID, ingestTree.ChangeSet.ShardVer)
	snap := ingestTree.ChangeSet.Snapshot
	for _, l0Create := range snap.L0Creates {
		file, err := en.fs.Open(l0Create.ID, fsOpts)
		if err != nil {
			return nil, nil, err
		}
		l0Tbl, err := sstable.OpenL0Table(file)
		if err != nil {
			return nil, nil, err
		}
		l0s.tables = append(l0s.tables, l0Tbl)
	}
	for _, tblCreate := range snap.TableCreates {
		handler := newHandlers[tblCreate.CF][tblCreate.Level-1]
		file, err := en.fs.Open(tblCreate.ID, fsOpts)
		if err != nil {
			return nil, nil, err
		}
		tbl, err := sstable.OpenTable(file, en.blkCache)
		if err != nil {
			return nil, nil, err
		}
		handler.totalSize += tbl.Size()
		handler.tables = append(handler.tables, tbl)
	}
	sort.Slice(l0s.tables, func(i, j int) bool {
		return l0s.tables[i].CommitTS() > l0s.tables[j].CommitTS()
	})
	for cf := 0; cf < en.numCFs; cf++ {
		for l := 1; l <= en.getCFMaxLevel(cf); l++ {
			handler := newHandlers[cf][l-1]
			sort.Slice(handler.tables, func(i, j int) bool {
				return bytes.Compare(handler.tables[i].Smallest(), handler.tables[j].Smallest()) < 0
			})
		}
	}
	return l0s, newHandlers, nil
}
