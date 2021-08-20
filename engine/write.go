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
	"encoding/binary"
	"github.com/ngaut/unistore/engine/table/memtable"
	"github.com/ngaut/unistore/engine/table/sstable"
	"github.com/ngaut/unistore/enginepb"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"os"
	"time"
)

type memTables struct {
	tables []*memtable.Table // tables from new to old, the first one is mutable.
}

type l0Tables struct {
	tables []*sstable.L0Table
}

func (sl0s *l0Tables) totalSize() int64 {
	var size int64
	for _, tbl := range sl0s.tables {
		size += tbl.Size()
	}
	return size
}

func (en *Engine) switchMemTable(shard *Shard, commitTS uint64) *memtable.Table {
	writableMemTbl := shard.loadWritableMemTable()
	if writableMemTbl == nil {
		writableMemTbl = memtable.NewCFTable(en.numCFs)
		atomicAddMemTable(shard.memTbls, writableMemTbl)
	}
	if writableMemTbl.Empty() {
		writableMemTbl = memtable.NewCFTable(en.numCFs)
	} else {
		newMemTable := memtable.NewCFTable(en.numCFs)
		atomicAddMemTable(shard.memTbls, newMemTable)
	}
	writableMemTbl.SetVersion(commitTS)
	log.S().Infof("shard %d:%d set mem-table version %d, empty %t, size %d",
		shard.ID, shard.Ver, commitTS, writableMemTbl.Empty(), writableMemTbl.Size())
	return writableMemTbl
}

func (en *Engine) Write(wb *WriteBatch) {
	shard := wb.shard
	commitTS := shard.baseTS + wb.sequence
	if shard.isSplitting() {
		if shard.ingestedPreSplitSeq == 0 || wb.sequence > shard.ingestedPreSplitSeq {
			en.writeSplitting(wb, commitTS)
			return
		}
		// Recover the shard to the pre-split stage when this shard is ingested.
	}
	memTbl := shard.loadWritableMemTable()
	if memTbl == nil || memTbl.Size()+wb.estimatedSize > shard.getMaxMemTableSize() {
		oldMemTbl := en.switchMemTable(shard, shard.loadMemTableTS())
		en.scheduleFlushTask(shard, oldMemTbl)
		memTbl = shard.loadWritableMemTable()
	}
	for cf, entries := range wb.entries {
		if !en.opt.CFs[cf].Managed {
			for _, entry := range entries {
				entry.Value.Version = commitTS
			}
		}
		memTbl.PutEntries(cf, entries)
	}
	for key, val := range wb.properties {
		shard.properties.set(key, val)
		if key == MemTableSizeKey {
			maxMemTableSize := int64(binary.LittleEndian.Uint64(val))
			shard.setMaxMemTableSize(maxMemTableSize)
			log.S().Infof("shard %d:%d mem size changed to %d", shard.ID, shard.Ver, maxMemTableSize)
		}
	}
}

func (en *Engine) writeSplitting(batch *WriteBatch, commitTS uint64) {
	for cf, entries := range batch.entries {
		if !en.opt.CFs[cf].Managed {
			for _, entry := range entries {
				entry.Value.Version = commitTS
			}
		}
		for _, entry := range entries {
			idx := getSplitShardIndex(batch.shard.splitKeys, entry.Key)
			memTbl := batch.shard.loadSplittingMemTable(idx)
			memTbl.Put(cf, entry.Key, entry.Value)
		}
	}
	for key, val := range batch.properties {
		batch.shard.properties.set(key, val)
	}
}

func (en *Engine) createL0File(fid uint64) (fd *os.File, err error) {
	filename := sstable.NewFilename(fid, en.opt.Dir)
	return y.OpenSyncedFile(filename, false)
}

func (en *Engine) GetProperty(shard *Shard, key string) (values []byte, ok bool) {
	return shard.properties.get(key)
}

func (en *Engine) scheduleFlushTask(shard *Shard, memTbl *memtable.Table) {
	lastSwitchTime := shard.lastSwitchTime
	shard.lastSwitchTime = time.Now()
	props := shard.properties.toPB(shard.ID)
	memTbl.SetProps(props)
	stage := shard.GetSplitStage()
	if stage == enginepb.SplitStage_PRE_SPLIT {
		stage = enginepb.SplitStage_PRE_SPLIT_FLUSH_DONE
	}
	memTbl.SetSplitStage(stage)
	if !shard.IsPassive() {
		var nextMemTblSize int64
		if !memTbl.Empty() && shard.IsInitialFlushed() {
			nextMemTblSize = shard.nextMemTableSize(memTbl.Size(), lastSwitchTime)
		}
		en.flushCh <- &flushTask{
			shard:       shard,
			tbl:         memTbl,
			nextMemSize: nextMemTblSize,
		}
	}
}
