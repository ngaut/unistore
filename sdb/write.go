package sdb

import (
	"github.com/ngaut/unistore/sdb/table/memtable"
	"github.com/ngaut/unistore/sdb/table/sstable"
	"github.com/ngaut/unistore/sdbpb"
	"github.com/pingcap/badger/y"
	"os"
)

const (
	bitDelete byte = 1 << 0 // Set if the key has been deleted.
)

type memTables struct {
	tables []*memtable.CFTable // tables from new to old, the first one is mutable.
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
	shard *Shard
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

func (sdb *DB) switchMemTable(shard *Shard, minSize int64, commitTS uint64) *memtable.CFTable {
	newTableSize := sdb.opt.MaxMemTableSize
	if newTableSize < minSize {
		newTableSize = minSize
	}
	writableMemTbl := shard.loadWritableMemTable()
	if writableMemTbl == nil {
		writableMemTbl = memtable.NewCFTable(newTableSize, sdb.numCFs)
		atomicAddMemTable(shard.memTbls, writableMemTbl)
	}
	empty := writableMemTbl.Empty()
	if empty {
		return nil
	}
	writableMemTbl.SetVersion(commitTS)
	newMemTable := memtable.NewCFTable(newTableSize, sdb.numCFs)
	atomicAddMemTable(shard.memTbls, newMemTable)
	return writableMemTbl
}

func (sdb *DB) executeWriteTask(eTask engineTask) {
	task := eTask.writeTask
	commitTS := sdb.orc.allocTs()
	defer func() {
		sdb.orc.doneCommit(commitTS)
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
	if memTbl == nil || (shard.IsInitialFlushed() && memTbl.Size()+task.estimatedSize > sdb.opt.MaxMemTableSize) {
		oldMemTbl := sdb.switchMemTable(shard, task.estimatedSize, commitTS)
		if oldMemTbl != nil {
			sdb.scheduleFlushTask(shard, oldMemTbl, commitTS, false)
		}
		memTbl = shard.loadWritableMemTable()
		// Update the commitTS so that the new memTable has a new commitTS, then
		// the old commitTS can be used as a snapshot at the memTable-switching time.
		sdb.orc.doneCommit(commitTS)
		commitTS = sdb.orc.allocTs()
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
	commitTS := sdb.orc.allocTs()
	memTbl := sdb.switchMemTable(shard, 0, commitTS)
	sdb.scheduleFlushTask(shard, memTbl, commitTS, shard.GetSplitState() == sdbpb.SplitState_PRE_SPLIT)
	sdb.orc.doneCommit(commitTS)
	eTask.notify <- nil
}

func (sdb *DB) scheduleFlushTask(shard *Shard, memTbl *memtable.CFTable, commitTS uint64, preSplitFlush bool) {
	if !shard.IsPassive() {
		sdb.flushCh <- &flushTask{
			shard:         shard,
			tbl:           memTbl,
			preSplitFlush: preSplitFlush,
			properties:    shard.properties.toPB(shard.ID),
			commitTS:      commitTS,
		}
	}
}
