package sdb

import (
	"github.com/pingcap/badger/fileutil"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"sync/atomic"
	"unsafe"
)

type shardFlushTask struct {
	shard         *Shard
	tbl           *memtable.CFTable
	preSplitFlush bool
	properties    *protos.ShardProperties
	commitTS      uint64

	finishSplitShards  []*Shard
	finishSplitMemTbls []*shardingMemTables
	finishSplitProps   []*protos.ShardProperties
}

func (sdb *ShardingDB) runFlushMemTable(c *y.Closer) {
	defer c.Done()
	for task := range sdb.flushCh {
		if len(task.finishSplitShards) > 0 {
			err := sdb.flushFinishSplit(task)
			if err != nil {
				panic(err)
			}
			continue
		}
		change := newShardChangeSet(task.shard)
		change.DataVer = task.commitTS
		change.Flush = &protos.ShardFlush{CommitTS: task.commitTS}
		if task.tbl != nil {
			l0Table, err := sdb.flushMemTable(task.shard, task.tbl, task.properties)
			if err != nil {
				// TODO: handle S3 error by queue the failed operation and retry.
				panic(err)
			}
			change.Flush.L0Create = l0Table
		}
		if task.preSplitFlush {
			change.State = protos.SplitState_PRE_SPLIT_FLUSH_DONE
		}
		if sdb.metaChangeListener != nil {
			sdb.metaChangeListener.OnChange(change)
		} else {
			err := sdb.applyFlush(task.shard, change)
			if err != nil {
				panic(err)
			}
		}
	}
}

func (sdb *ShardingDB) flushFinishSplit(task *shardFlushTask) error {
	log.S().Info("flush finish split")
	if atomic.LoadUint32(&sdb.closed) == 1 {
		return nil
	}
	for idx, memTbls := range task.finishSplitMemTbls {
		flushChangeSet := newShardChangeSet(task.finishSplitShards[idx])
		flushChangeSet.Flush = &protos.ShardFlush{CommitTS: task.commitTS}
		y.Assert(len(memTbls.tables) <= 1)
		if len(memTbls.tables) == 1 {
			memTbl := memTbls.tables[0]
			l0Table, err := sdb.flushMemTable(task.finishSplitShards[idx], memTbl, task.finishSplitProps[idx])
			if err != nil {
				// TODO: handle s3 error by queue the failed operation and retry.
				panic(err)
			}
			l0Table.Properties = task.finishSplitProps[idx]
			flushChangeSet.Flush.L0Create = l0Table
		}
		if sdb.metaChangeListener != nil {
			sdb.metaChangeListener.OnChange(flushChangeSet)
		} else {
			err := sdb.ApplyChangeSet(flushChangeSet)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (sdb *ShardingDB) flushMemTable(shard *Shard, m *memtable.CFTable, props *protos.ShardProperties) (*protos.L0Create, error) {
	y.Assert(sdb.idAlloc != nil)
	id := sdb.idAlloc.AllocID()
	log.S().Infof("flush memtable id:%d, size:%d", id, m.Size())
	fd, err := sdb.createL0File(id)
	if err != nil {
		return nil, err
	}
	writer := fileutil.NewBufferedWriter(fd, sdb.opt.TableBuilderOptions.WriteBufferSize, nil)
	builder := newShardL0Builder(sdb.numCFs, sdb.opt.TableBuilderOptions, m.GetVersion())
	for cf := 0; cf < sdb.numCFs; cf++ {
		it := m.NewIterator(cf, false)
		if it == nil {
			continue
		}
		for it.Rewind(); it.Valid(); y.NextAllVersion(it) {
			builder.Add(cf, it.Key(), it.Value())
		}
		it.Close()
	}
	shardL0Data := builder.Finish()
	_, err = writer.Write(shardL0Data)
	if err != nil {
		return nil, err
	}
	err = writer.Finish()
	if err != nil {
		return nil, err
	}
	filename := fd.Name()
	_ = fd.Close()
	result := &sstable.BuildResult{
		FileName: filename,
		Smallest: y.KeyWithTs(shard.Start, 0),
		Biggest:  y.KeyWithTs(shard.End, 0),
	}
	if sdb.s3c != nil {
		err = putSSTBuildResultToS3(sdb.s3c, result)
		if err != nil {
			// TODO: handle this error by queue the failed operation and retry.
			return nil, err
		}
	}
	return newL0CreateByResult(result, props), nil
}

func atomicAddMemTable(pointer *unsafe.Pointer, memTbl *memtable.CFTable) {
	for {
		oldMemTbls := (*shardingMemTables)(atomic.LoadPointer(pointer))
		newMemTbls := &shardingMemTables{make([]*memtable.CFTable, 0, len(oldMemTbls.tables)+1)}
		newMemTbls.tables = append(newMemTbls.tables, memTbl)
		newMemTbls.tables = append(newMemTbls.tables, oldMemTbls.tables...)
		if atomic.CompareAndSwapPointer(pointer, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
			break
		}
	}
}

func atomicRemoveMemTable(pointer *unsafe.Pointer, cnt int) {
	if cnt == 0 {
		return
	}
	for {
		oldMemTbls := (*shardingMemTables)(atomic.LoadPointer(pointer))
		// When we recover flush, the mem-table is empty, newLen maybe negative.
		newLen := len(oldMemTbls.tables) - cnt
		if newLen < 0 {
			newLen = 0
		}
		newMemTbls := &shardingMemTables{make([]*memtable.CFTable, newLen)}
		copy(newMemTbls.tables, oldMemTbls.tables)
		if atomic.CompareAndSwapPointer(pointer, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
			break
		}
	}
}

func atomicAddL0(shard *Shard, l0Tbls ...*shardL0Table) {
	if len(l0Tbls) == 0 {
		return
	}
	pointer := shard.l0s
	for {
		oldL0Tbls := (*shardL0Tables)(atomic.LoadPointer(pointer))
		newL0Tbls := &shardL0Tables{make([]*shardL0Table, 0, len(oldL0Tbls.tables)+1)}
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
		oldL0Tbls := (*shardL0Tables)(atomic.LoadPointer(pointer))
		for i := len(oldL0Tbls.tables) - cnt; i < len(oldL0Tbls.tables); i++ {
			size += oldL0Tbls.tables[i].size
		}
		newL0Tbls := &shardL0Tables{make([]*shardL0Table, len(oldL0Tbls.tables)-cnt)}
		copy(newL0Tbls.tables, oldL0Tbls.tables)
		if atomic.CompareAndSwapPointer(pointer, unsafe.Pointer(oldL0Tbls), unsafe.Pointer(newL0Tbls)) {
			log.S().Infof("shard %d:%d atomic removed %d l0s", shard.ID, shard.Ver, cnt)
			return size
		}
	}
}
