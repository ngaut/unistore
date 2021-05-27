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

type engineTask struct {
	writeTask       *WriteBatch
	preSplitTask    *preSplitTask
	finishSplitTask *finishSplitTask
	getProperties   *getPropertyTask
	triggerFlush    *triggerFlushTask
	notify          chan error
}

type preSplitTask struct {
	shard *Shard
	keys  [][]byte
}

type finishSplitTask struct {
	shard     *Shard
	newProps  []*sdbpb.Properties
	newShards []*Shard
}

type getPropertyTask struct {
	shard  *Shard
	keys   []string
	values [][]byte
}

type triggerFlushTask struct {
	shard   *Shard
	skipCnt int
}

func (sdb *DB) runWriteLoop(closer *y.Closer) {
	defer closer.Done()
	for {
		tasks := sdb.collectTasks(closer)
		if len(tasks) == 0 {
			return
		}
		for _, task := range tasks {
			if task.writeTask != nil {
				sdb.executeWriteTask(task)
			}
			if task.preSplitTask != nil {
				sdb.executePreSplitTask(task)
			}
			if task.finishSplitTask != nil {
				sdb.executeFinishSplitTask(task)
			}
			if task.getProperties != nil {
				sdb.executeGetPropertiesTask(task)
			}
			if task.triggerFlush != nil {
				sdb.executeTriggerFlushTask(task)
			}
		}
	}
}

func (sdb *DB) collectTasks(c *y.Closer) []engineTask {
	var engineTasks []engineTask
	select {
	case x := <-sdb.writeCh:
		engineTasks = append(engineTasks, x)
		l := len(sdb.writeCh)
		for i := 0; i < l; i++ {
			engineTasks = append(engineTasks, <-sdb.writeCh)
		}
	case <-c.HasBeenClosed():
		return nil
	}
	return engineTasks
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

func (sdb *DB) executeWriteTask(eTask engineTask) {
	task := eTask.writeTask
	commitTS := task.shard.allocCommitTS()
	defer func() {
		if len(eTask.notify) == 0 {
			eTask.notify <- nil
		}
	}()
	shard := task.shard
	latest := sdb.GetShard(shard.ID)
	if latest.Ver != shard.Ver {
		eTask.notify <- errShardNotMatch
		return
	}
	if shard.isSplitting() {
		commitTS = sdb.writeSplitting(task, commitTS)
		return
	}
	memTbl := shard.loadWritableMemTable()
	if memTbl == nil || memTbl.Size()+task.estimatedSize > shard.maxMemTableSize {
		oldMemTbl := sdb.switchMemTable(shard, commitTS)
		sdb.scheduleFlushTask(shard, oldMemTbl)
		memTbl = shard.loadWritableMemTable()
		// Update the commitTS so that the new memTable has a new commitTS, then
		// the old commitTS can be used as a snapshot at the memTable-switching time.
		commitTS = shard.allocCommitTS()
	}
	for cf, entries := range task.entries {
		if !sdb.opt.CFs[cf].Managed {
			for _, entry := range entries {
				entry.Value.Version = commitTS
			}
		}
		memTbl.PutEntries(cf, entries)
	}
	for key, val := range task.properties {
		shard.properties.set(key, val)
		if key == MemTableSizeKey {
			shard.maxMemTableSize = int64(binary.LittleEndian.Uint64(val))
		}
	}
}

func (sdb *DB) writeSplitting(batch *WriteBatch, commitTS uint64) uint64 {
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
	return commitTS
}

func (sdb *DB) createL0File(fid uint64) (fd *os.File, err error) {
	filename := sstable.NewFilename(fid, sdb.opt.Dir)
	return y.OpenSyncedFile(filename, false)
}

func (sdb *DB) executeGetPropertiesTask(eTask engineTask) {
	task := eTask.getProperties
	for _, key := range task.keys {
		val, _ := task.shard.properties.get(key)
		task.values = append(task.values, val)
	}
	eTask.notify <- nil
}

func (sdb *DB) executeTriggerFlushTask(eTask engineTask) {
	task := eTask.triggerFlush
	shard := task.shard
	mems := shard.loadMemTables()
	for i := len(mems.tables) - task.skipCnt - 1; i > 0; i-- {
		memTbl := mems.tables[i]
		sdb.flushCh <- &flushTask{
			shard: shard,
			tbl:   memTbl,
		}
	}
	if len(mems.tables) == 1 && mems.tables[0].Empty() {
		if !shard.IsInitialFlushed() {
			commitTS := shard.allocCommitTS()
			memTbl := sdb.switchMemTable(shard, commitTS)
			sdb.flushCh <- &flushTask{
				shard: shard,
				tbl:   memTbl,
			}
		}
	}
	eTask.notify <- nil
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
