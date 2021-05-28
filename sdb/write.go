package sdb

import (
	"encoding/binary"
	"github.com/ngaut/unistore/sdb/table/memtable"
	"github.com/ngaut/unistore/sdb/table/sstable"
	"github.com/ngaut/unistore/sdbpb"
	"github.com/pingcap/badger/y"
	"os"
	"time"
)

const (
	bitDelete byte = 1 << 0 // Set if the key has been deleted.
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

func (sdb *DB) switchMemTable(shard *Shard, commitTS uint64) *memtable.Table {
	writableMemTbl := shard.loadWritableMemTable()
	if writableMemTbl == nil {
		writableMemTbl = memtable.NewCFTable(sdb.numCFs)
		atomicAddMemTable(shard.memTbls, writableMemTbl)
	}
	if writableMemTbl.Empty() {
		writableMemTbl = memtable.NewCFTable(sdb.numCFs)
	} else {
		newMemTable := memtable.NewCFTable(sdb.numCFs)
		atomicAddMemTable(shard.memTbls, newMemTable)
	}
	writableMemTbl.SetVersion(commitTS)
	return writableMemTbl
}

func (sdb *DB) Write(wb *WriteBatch) {
	commitTS := wb.shard.allocCommitTS()
	shard := wb.shard
	if shard.isSplitting() {
		sdb.writeSplitting(wb, commitTS)
		return
	}
	memTbl := shard.loadWritableMemTable()
	if memTbl == nil || memTbl.Size()+wb.estimatedSize > shard.maxMemTableSize {
		oldMemTbl := sdb.switchMemTable(shard, commitTS)
		sdb.scheduleFlushTask(shard, oldMemTbl)
		memTbl = shard.loadWritableMemTable()
		// Update the commitTS so that the new memTable has a new commitTS, then
		// the old commitTS can be used as a snapshot at the memTable-switching time.
		commitTS = shard.allocCommitTS()
	}
	for cf, entries := range wb.entries {
		if !sdb.opt.CFs[cf].Managed {
			for _, entry := range entries {
				entry.Value.Version = commitTS
			}
		}
		memTbl.PutEntries(cf, entries)
	}
	for key, val := range wb.properties {
		shard.properties.set(key, val)
		if key == MemTableSizeKey {
			shard.maxMemTableSize = int64(binary.LittleEndian.Uint64(val))
		}
	}
}

func (sdb *DB) writeSplitting(batch *WriteBatch, commitTS uint64) {
	for cf, entries := range batch.entries {
		if !sdb.opt.CFs[cf].Managed {
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

func (sdb *DB) createL0File(fid uint64) (fd *os.File, err error) {
	filename := sstable.NewFilename(fid, sdb.opt.Dir)
	return y.OpenSyncedFile(filename, false)
}

func (sdb *DB) GetProperty(shard *Shard, key string) (values []byte, ok bool) {
	return shard.properties.get(key)
}

func (sdb *DB) scheduleFlushTask(shard *Shard, memTbl *memtable.Table) {
	lastSwitchTime := shard.lastSwitchTime
	shard.lastSwitchTime = time.Now()
	props := shard.properties.toPB(shard.ID)
	var nextMemTblSize int64
	if !memTbl.Empty() && shard.IsInitialFlushed() {
		nextMemTblSize = shard.nextMemTableSize(memTbl.Size(), lastSwitchTime)
	}
	memTbl.SetProps(props)
	stage := shard.GetSplitStage()
	if stage == sdbpb.SplitStage_PRE_SPLIT {
		stage = sdbpb.SplitStage_PRE_SPLIT_FLUSH_DONE
	}
	memTbl.SetSplitStage(stage)
	if !shard.IsPassive() {
		sdb.flushCh <- &flushTask{
			shard:       shard,
			tbl:         memTbl,
			nextMemSize: nextMemTblSize,
		}
	}
}
