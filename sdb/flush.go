package sdb

import (
	"github.com/ngaut/unistore/sdb/fileutil"
	"github.com/ngaut/unistore/sdb/table/memtable"
	"github.com/ngaut/unistore/sdb/table/sstable"
	"github.com/ngaut/unistore/sdbpb"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"sync/atomic"
	"unsafe"
)

type flushTask struct {
	shard         *Shard
	tbl           *memtable.CFTable
	preSplitFlush bool
	properties    *sdbpb.Properties
	commitTS      uint64

	finishSplitShards  []*Shard
	finishSplitMemTbls []*memTables
	finishSplitProps   []*sdbpb.Properties
}

func (sdb *DB) runFlushMemTable(c *y.Closer) {
	defer c.Done()
	for task := range sdb.flushCh {
		if len(task.finishSplitShards) > 0 {
			err := sdb.flushFinishSplit(task)
			if err != nil {
				panic(err)
			}
			continue
		}
		change := newChangeSet(task.shard)
		change.DataVer = task.commitTS
		change.Flush = &sdbpb.Flush{CommitTS: task.commitTS}
		if task.tbl != nil {
			l0Table, err := sdb.flushMemTable(task.shard, task.tbl, task.properties)
			if err != nil {
				// TODO: handle S3 error by queue the failed operation and retry.
				panic(err)
			}
			change.Flush.L0Create = l0Table
		}
		if task.preSplitFlush {
			change.State = sdbpb.SplitState_PRE_SPLIT_FLUSH_DONE
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

func (sdb *DB) flushFinishSplit(task *flushTask) error {
	log.S().Info("flush finish split")
	if atomic.LoadUint32(&sdb.closed) == 1 {
		return nil
	}
	for idx, memTbls := range task.finishSplitMemTbls {
		flushChangeSet := newChangeSet(task.finishSplitShards[idx])
		flushChangeSet.Flush = &sdbpb.Flush{CommitTS: task.commitTS}
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

func (sdb *DB) flushMemTable(shard *Shard, m *memtable.CFTable, props *sdbpb.Properties) (*sdbpb.L0Create, error) {
	y.Assert(sdb.idAlloc != nil)
	id := sdb.idAlloc.AllocID()
	fd, err := sdb.createL0File(id)
	if err != nil {
		return nil, err
	}
	writer := fileutil.NewBufferedWriter(fd, sdb.opt.TableBuilderOptions.WriteBufferSize, nil)
	builder := sstable.NewL0Builder(sdb.numCFs, id, sdb.opt.TableBuilderOptions, m.GetVersion())
	for cf := 0; cf < sdb.numCFs; cf++ {
		it := m.NewIterator(cf, false)
		if it == nil {
			continue
		}
		rc := sdb.opt.CFs[cf].ReadCommitted
		var prevKey y.Key
		for it.Rewind(); it.Valid(); y.NextAllVersion(it) {
			if rc && prevKey.SameUserKey(it.Key()) {
				// For read committed CF, we can discard all the old versions.
				continue
			}
			builder.Add(cf, it.Key(), it.Value())
			if rc {
				prevKey.Copy(it.Key())
			}
		}
		it.Close()
	}
	shardL0Data := builder.Finish()
	log.S().Infof("flush memtable id:%d, size:%d, l0 size: %d", id, m.Size(), len(shardL0Data))
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
		Smallest: shard.Start,
		Biggest:  shard.End,
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
		oldMemTbls := (*memTables)(atomic.LoadPointer(pointer))
		newMemTbls := &memTables{make([]*memtable.CFTable, 0, len(oldMemTbls.tables)+1)}
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
		oldMemTbls := (*memTables)(atomic.LoadPointer(pointer))
		// When we recover flush, the mem-table is empty, newLen maybe negative.
		newLen := len(oldMemTbls.tables) - cnt
		if newLen < 0 {
			newLen = 0
		}
		newMemTbls := &memTables{make([]*memtable.CFTable, newLen)}
		copy(newMemTbls.tables, oldMemTbls.tables)
		if atomic.CompareAndSwapPointer(pointer, unsafe.Pointer(oldMemTbls), unsafe.Pointer(newMemTbls)) {
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
