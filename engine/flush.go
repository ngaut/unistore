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
	"github.com/ngaut/unistore/engine/fileutil"
	"github.com/ngaut/unistore/engine/table"
	"github.com/ngaut/unistore/engine/table/memtable"
	"github.com/ngaut/unistore/engine/table/sstable"
	"github.com/ngaut/unistore/enginepb"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"sync"
	"sync/atomic"
	"unsafe"
)

type flushTask struct {
	shard       *Shard
	tbl         *memtable.Table
	nextMemSize int64
}

type flushResultTask struct {
	*flushTask
	change *enginepb.ChangeSet
	wg     *sync.WaitGroup
	err    error
}

func (en *Engine) runFlushMemTable(c *y.Closer) {
	defer c.Done()
	for task := range en.flushCh {
		change := newChangeSet(task.shard)
		resultTask := &flushResultTask{
			wg:        &sync.WaitGroup{},
			flushTask: task,
			change:    change,
		}
		change.Flush = &enginepb.Flush{Properties: task.tbl.GetProps(), CommitTS: task.tbl.GetVersion()}
		change.Stage = task.tbl.GetSplitStage()
		if !task.tbl.Empty() {
			resultTask.wg.Add(1)
			l0Table, err := en.flushMemTable(task.shard, task.tbl, resultTask)
			if err != nil {
				// TODO: handle DFS error by queue the failed operation and retry.
				panic(err)
			}
			change.Flush.L0Create = l0Table
		}
		en.flushResultCh <- resultTask
	}
}

func (en *Engine) runFlushResult(c *y.Closer) {
	defer c.Done()
	for task := range en.flushResultCh {
		task.wg.Wait()
		if task.err != nil {
			// TODO: handle DFS error by queue the failed operation and retry.
			panic(task.err)
		}
		if en.metaChangeListener != nil {
			en.metaChangeListener.OnChange(task.change)
			if task.nextMemSize > 0 {
				changeSize := newChangeSet(task.shard)
				changeSize.NextMemTableSize = task.nextMemSize
				en.metaChangeListener.OnChange(changeSize)
			}
		} else {
			err := en.applyFlush(task.shard, task.change)
			if err != nil {
				panic(err)
			}
		}
	}
}

func (en *Engine) flushMemTable(shard *Shard, m *memtable.Table, resultTask *flushResultTask) (*enginepb.L0Create, error) {
	y.Assert(en.idAlloc != nil)
	id, err := en.idAlloc.AllocID(1)
	if err != nil {
		return nil, err
	}
	fd, err := en.createL0File(id)
	if err != nil {
		return nil, err
	}
	writer := fileutil.NewBufferedWriter(fd, en.opt.TableBuilderOptions.WriteBufferSize, nil)
	builder := sstable.NewL0Builder(en.numCFs, id, en.opt.TableBuilderOptions, m.GetVersion())
	for cf := 0; cf < en.numCFs; cf++ {
		it := m.NewIterator(cf, false)
		if it == nil {
			continue
		}
		// If CF is not managed, we only need to keep the latest version.
		rc := !en.opt.CFs[cf].Managed
		var prevKey []byte
		for it.Rewind(); it.Valid(); table.NextAllVersion(it) {
			if rc && bytes.Equal(prevKey, it.Key()) {
				// For read committed CF, we can discard all the old versions.
				continue
			}
			builder.Add(cf, it.Key(), it.Value())
			if rc {
				prevKey = append(prevKey[:0], it.Key()...)
			}
		}
		it.Close()
	}
	shardL0Data := builder.Finish()
	smallest, biggest := builder.SmallestAndBiggest()
	log.S().Infof("%d:%d flush memtable id:%d, size:%d, l0 size: %d, props:%s, commitTS: %d",
		shard.ID, shard.Ver, id, m.Size(), len(shardL0Data), newProperties().applyPB(m.GetProps()), m.GetVersion())
	_, err = writer.Write(shardL0Data)
	if err != nil {
		return nil, err
	}
	err = writer.Finish()
	if err != nil {
		return nil, err
	}
	_ = fd.Close()
	result := &sstable.BuildResult{
		ID:       id,
		FileData: shardL0Data,
		Smallest: smallest,
		Biggest:  biggest,
	}
	en.fs.GetScheduler().Schedule(func() {
		resultTask.err = en.fs.Create(result.ID, result.FileData, dfs.NewOptions(shard.ID, shard.Ver))
		resultTask.wg.Done()
	})
	return newL0CreateByResult(result), nil
}

func atomicAddMemTable(pointer *unsafe.Pointer, memTbl *memtable.Table) {
	for {
		oldMemTbls := (*memTables)(atomic.LoadPointer(pointer))
		newMemTbls := &memTables{make([]*memtable.Table, 0, len(oldMemTbls.tables)+1)}
		newMemTbls.tables = append(newMemTbls.tables, memTbl)
		newMemTbls.tables = append(newMemTbls.tables, oldMemTbls.tables...)
		if atomic.CompareAndSwapPointer(pointer, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
			break
		}
	}
}

func atomicRemoveMemTable(shard *Shard, commitTS uint64) {
	pointer := shard.memTbls
	for {
		var lastMemTbl *memtable.Table
		oldMemTbls := (*memTables)(atomic.LoadPointer(pointer))
		// When we recover flush, the mem-table is empty, newLen maybe negative.
		newLen := len(oldMemTbls.tables) - 1
		if newLen < 1 {
			return
		} else {
			lastMemTbl = oldMemTbls.tables[len(oldMemTbls.tables)-1]
			if lastMemTbl.GetVersion() < commitTS {
				log.S().Panicf("shard %d:%d missing flush, last mem-table ts %d, remove commitTS %d", shard.ID, shard.Ver, lastMemTbl.GetVersion(), commitTS)
			} else if lastMemTbl.GetVersion() > commitTS {
				return
			}
		}
		newMemTbls := &memTables{make([]*memtable.Table, newLen)}
		copy(newMemTbls.tables, oldMemTbls.tables)
		if atomic.CompareAndSwapPointer(pointer, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
			if lastMemTbl != nil {
				log.S().Infof("shard %d:%d atomic removed mem-table version %d, size %d", shard.ID, shard.Ver, lastMemTbl.GetVersion(), lastMemTbl.Size())
			}
			break
		}
	}
}

func atomicAddL0(shard *Shard, l0Tbls ...*sstable.L0Table) {
	if len(l0Tbls) == 0 {
		return
	}
	pointer := shard.l0s
	for {
		oldL0Tbls := (*l0Tables)(atomic.LoadPointer(pointer))
		newL0Tbls := &l0Tables{make([]*sstable.L0Table, 0, len(oldL0Tbls.tables)+1)}
		newL0Tbls.tables = append(newL0Tbls.tables, l0Tbls...)
		newL0Tbls.tables = append(newL0Tbls.tables, oldL0Tbls.tables...)
		if atomic.CompareAndSwapPointer(pointer, unsafe.Pointer(oldL0Tbls), unsafe.Pointer(newL0Tbls)) {
			log.S().Infof("shard %d:%d added %d l0s", shard.ID, shard.Ver, len(l0Tbls))
			break
		}
	}
}

func atomicRemoveL0(shard *Shard, cnt int) int64 {
	pointer := shard.l0s
	for {
		var size int64
		oldL0Tbls := (*l0Tables)(atomic.LoadPointer(pointer))
		for i := len(oldL0Tbls.tables) - cnt; i < len(oldL0Tbls.tables); i++ {
			size += oldL0Tbls.tables[i].Size()
		}
		newL0Tbls := &l0Tables{make([]*sstable.L0Table, len(oldL0Tbls.tables)-cnt)}
		copy(newL0Tbls.tables, oldL0Tbls.tables)
		if atomic.CompareAndSwapPointer(pointer, unsafe.Pointer(oldL0Tbls), unsafe.Pointer(newL0Tbls)) {
			log.S().Infof("shard %d:%d atomic removed %d l0s", shard.ID, shard.Ver, cnt)
			return size
		}
	}
}
