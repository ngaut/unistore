package sdb

import (
	"bytes"
	"fmt"
	"github.com/ngaut/unistore/sdb/table/memtable"
	"github.com/ngaut/unistore/sdbpb"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ngaut/unistore/s3util"
	"github.com/ngaut/unistore/sdb/fileutil"
	"github.com/ngaut/unistore/sdb/table"
	"github.com/ngaut/unistore/sdb/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

/*
Shard split can be performed in 3 steps:
1. PreSplit
	Set the split keys of the shard, then all new entries are written to separated mem-tables.
    This operation also
*/

// PreSplit sets the split keys, then all new entries are written to separated mem-tables.
func (sdb *DB) PreSplit(shardID, ver uint64, keys [][]byte) error {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	shard := sdb.GetShard(shardID)
	if shard == nil {
		return errShardNotFound
	}
	if shard.Ver != ver {
		log.Info("shard not match", zap.Uint64("current", shard.Ver), zap.Uint64("request", ver))
		return errShardNotMatch
	}
	if !shard.setSplitKeys(keys) {
		return errors.New("failed to set split keys")
	}
	change := newChangeSet(shard)
	change.Stage = sdbpb.SplitStage_PRE_SPLIT
	change.PreSplit = &sdbpb.PreSplit{
		Keys:     keys,
		MemProps: shard.properties.toPB(shard.ID),
	}
	err := sdb.manifest.writeChangeSet(change)
	if err != nil {
		return err
	}
	commitTS := shard.allocCommitTS()
	memTbl := sdb.switchMemTable(shard, commitTS)
	sdb.scheduleFlushTask(shard, memTbl)
	return nil
}

// SplitShardFiles splits the files that overlaps the split keys.
func (sdb *DB) SplitShardFiles(shardID, ver uint64) (*sdbpb.ChangeSet, error) {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	shard := sdb.GetShard(shardID)
	if shard == nil {
		return nil, errShardNotFound
	}
	if shard.Ver != ver {
		log.Info("shard not match", zap.Uint64("current", shard.Ver), zap.Uint64("request", ver))
		return nil, errShardNotMatch
	}
	if !shard.isSplitting() {
		log.S().Infof("wrong splitting stage %s", shard.GetSplitStage())
		return nil, errShardWrongSplittingStage
	}
	change := newChangeSet(shard)
	change.SplitFiles = &sdbpb.SplitFiles{}
	change.Stage = sdbpb.SplitStage_SPLIT_FILE_DONE
	shard.lock.Lock()
	defer shard.lock.Unlock()
	keys := shard.splitKeys
	sdb.waitForPreSplitFlushState(shard)
	err := sdb.splitShardL0Tables(shard, change.SplitFiles)
	if err != nil {
		return nil, err
	}
	for cf := 0; cf < sdb.numCFs; cf++ {
		for lvl := 1; lvl <= ShardMaxLevel; lvl++ {
			if err = sdb.splitTables(shard, cf, lvl, keys, change.SplitFiles); err != nil {
				return nil, err
			}
		}
	}
	return change, nil
}

func (sdb *DB) waitForPreSplitFlushState(shard *Shard) {
	for {
		switch shard.GetSplitStage() {
		case sdbpb.SplitStage_PRE_SPLIT_FLUSH_DONE, sdbpb.SplitStage_SPLIT_FILE_DONE:
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (sdb *DB) splitShardL0Tables(shard *Shard, splitFiles *sdbpb.SplitFiles) error {
	l0s := shard.loadL0Tables()
	for i := 0; i < len(l0s.tables); i++ {
		l0Tbl := l0s.tables[i]
		newL0s, err := sdb.splitShardL0Table(shard, l0Tbl)
		if err != nil {
			return err
		}
		splitFiles.L0Creates = append(splitFiles.L0Creates, newL0s...)
		splitFiles.TableDeletes = append(splitFiles.TableDeletes, l0Tbl.ID())
	}
	return nil
}

func (sdb *DB) splitShardL0Table(shard *Shard, l0 *sstable.L0Table) ([]*sdbpb.L0Create, error) {
	iters := make([]table.Iterator, sdb.numCFs)
	for cf := 0; cf < sdb.numCFs; cf++ {
		iters[cf] = l0.NewIterator(cf, false)
		if iters[cf] != nil {
			it := iters[cf]
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
			}
			it.Rewind()
		}
	}
	var newL0s []*sdbpb.L0Create
	var bt *s3util.BatchTasks
	if sdb.s3c != nil {
		bt = s3util.NewBatchTasks()
	}
	for i, key := range shard.splitKeys {
		startKey := shard.Start
		if i != 0 {
			startKey = shard.splitKeys[i-1]
		}
		result, err := sdb.buildShardL0BeforeKey(iters, startKey, key, l0.CommitTS())
		if err != nil {
			return nil, err
		}
		if result != nil {
			if sdb.s3c != nil {
				bt.AppendTask(func() error {
					return putSSTBuildResultToS3(sdb.s3c, result)
				})
			}
			newL0 := newL0CreateByResult(result, nil)
			newL0s = append(newL0s, newL0)
		}
	}
	result, err := sdb.buildShardL0BeforeKey(iters, shard.splitKeys[len(shard.splitKeys)-1], shard.End, l0.CommitTS())
	if err != nil {
		return nil, err
	}
	if result != nil {
		if sdb.s3c != nil {
			bt.AppendTask(func() error {
				return putSSTBuildResultToS3(sdb.s3c, result)
			})
		}
		lastL0 := newL0CreateByResult(result, nil)
		newL0s = append(newL0s, lastL0)
	}
	if sdb.s3c != nil {
		if err := sdb.s3c.BatchSchedule(bt); err != nil {
			return nil, err
		}
	}
	return newL0s, nil
}

func (sdb *DB) buildShardL0BeforeKey(iters []table.Iterator, startKey, endKey []byte, commitTS uint64) (*sstable.BuildResult, error) {
	fid := sdb.idAlloc.AllocID()
	builder := sstable.NewL0Builder(sdb.numCFs, fid, sdb.opt.TableBuilderOptions, commitTS)
	var hasData bool
	for cf := 0; cf < sdb.numCFs; cf++ {
		iter := iters[cf]
		if iter == nil {
			continue
		}
		for ; iter.Valid(); table.NextAllVersion(iter) {
			if bytes.Compare(iter.Key(), endKey) >= 0 {
				break
			}
			builder.Add(cf, iter.Key(), iter.Value())
			hasData = true
		}
	}
	if !hasData {
		return nil, nil
	}
	shardL0Data := builder.Finish()
	fd, err := sdb.createL0File(fid)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	writer := fileutil.NewBufferedWriter(fd, sdb.opt.TableBuilderOptions.WriteBufferSize, nil)
	_, err = writer.Write(shardL0Data)
	if err != nil {
		return nil, err
	}
	err = writer.Finish()
	if err != nil {
		return nil, err
	}
	result := &sstable.BuildResult{
		FileName: fd.Name(),
		FileData: shardL0Data,
		Smallest: startKey,
		Biggest:  endKey,
	}
	return result, nil
}

func (sdb *DB) splitTables(shard *Shard, cf int, level int, keys [][]byte, splitFiles *sdbpb.SplitFiles) error {
	scf := shard.cfs[cf]
	oldHandler := scf.getLevelHandler(level)
	oldTables := oldHandler.tables
	toDeleteIDs := make(map[uint64]struct{})
	var relatedKeys [][]byte
	var bt *s3util.BatchTasks
	if sdb.s3c != nil {
		bt = s3util.NewBatchTasks()
	}
	for _, tbl := range oldTables {
		relatedKeys = relatedKeys[:0]
		for _, key := range keys {
			if bytes.Compare(tbl.Smallest(), key) < 0 &&
				bytes.Compare(key, tbl.Biggest()) <= 0 {
				relatedKeys = append(relatedKeys, key)
			}
		}
		if len(relatedKeys) == 0 {
			continue
		}
		toDeleteIDs[tbl.ID()] = struct{}{}
		// append an end key to build the last table.
		relatedKeys = append(relatedKeys, globalShardEndKey)
		itr := tbl.NewIterator(false)
		defer itr.Close()
		itr.Rewind()
		for _, relatedKey := range relatedKeys {
			result, err := sdb.buildTableBeforeKey(itr, relatedKey, level, sdb.opt.TableBuilderOptions)
			if err != nil {
				return err
			}
			if result != nil {
				if sdb.s3c != nil {
					bt.AppendTask(func() error {
						return putSSTBuildResultToS3(sdb.s3c, result)
					})
				}
				splitFiles.TableCreates = append(splitFiles.TableCreates, newTableCreateByResult(result, cf, level))
			}
		}
		splitFiles.TableDeletes = append(splitFiles.TableDeletes, tbl.ID())
	}
	if sdb.s3c != nil {
		if err := sdb.s3c.BatchSchedule(bt); err != nil {
			return err
		}
	}
	return nil
}

func (sdb *DB) buildTableBeforeKey(itr table.Iterator, key []byte, level int, opt sstable.TableBuilderOptions) (*sstable.BuildResult, error) {
	id := sdb.idAlloc.AllocID()
	filename := sstable.NewFilename(id, sdb.opt.Dir)
	fd, err := y.OpenSyncedFile(filename, false)
	if err != nil {
		return nil, err
	}
	b := sstable.NewTableBuilder(id, opt)
	for itr.Valid() {
		if len(key) > 0 && bytes.Compare(itr.Key(), key) >= 0 {
			break
		}
		val := itr.Value()
		b.Add(itr.Key(), &val)
		table.NextAllVersion(itr)
	}
	if b.Empty() {
		return nil, nil
	}
	result, err1 := b.Finish(fd.Name(), fd)
	if err1 != nil {
		return nil, err1
	}
	return result, nil
}

// FinishSplit finishes the Split process on a Shard in PreSplitStage.
// This is done after preSplit is done, so we don't need to acquire any lock, just atomic CAS will do.
func (sdb *DB) FinishSplit(oldShardID, ver uint64, newShardsProps []*sdbpb.Properties) (newShards []*Shard, err error) {
	oldShard := sdb.GetShard(oldShardID)
	if oldShard.Ver != ver {
		return nil, errShardNotMatch
	}
	if !oldShard.isSplitting() {
		return nil, errors.New("shard is not in splitting stage")
	}
	if len(newShardsProps) != len(oldShard.splittingMemTbls) {
		return nil, fmt.Errorf("newShardsProps length %d is not equals to splittingMemTbls length %d", len(newShardsProps), len(oldShard.splittingMemTbls))
	}
	changeSet := newChangeSet(oldShard)
	changeSet.Split = &sdbpb.Split{
		NewShards: newShardsProps,
		Keys:      oldShard.splitKeys,
		MemProps:  oldShard.properties.toPB(oldShard.ID),
	}
	err = sdb.manifest.writeChangeSet(changeSet)
	if err != nil {
		return nil, err
	}
	newShards = sdb.buildSplitShards(oldShard, newShardsProps)
	return
}

func (sdb *DB) buildSplitShards(oldShard *Shard, newShardsProps []*sdbpb.Properties) (newShards []*Shard) {
	newShards = make([]*Shard, len(oldShard.splittingMemTbls))
	newVer := oldShard.Ver + uint64(len(newShardsProps)) - 1
	commitTS := oldShard.allocCommitTS()
	for i := range oldShard.splittingMemTbls {
		startKey, endKey := getSplittingStartEnd(oldShard.Start, oldShard.End, oldShard.splitKeys, i)
		shard := newShard(newShardsProps[i], newVer, startKey, endKey, &sdb.opt, sdb.metrics)
		if oldShard.IsPassive() || shard.ID != oldShard.ID {
			// If the shard is not derived shard, the flush will be triggered later when the new shard elected a leader.
			shard.SetPassive(true)
		}
		shard.memTbls = new(unsafe.Pointer)
		atomic.StorePointer(shard.memTbls, unsafe.Pointer(&memTables{tables: []*memtable.Table{oldShard.loadSplittingMemTable(i)}}))
		shard.l0s = new(unsafe.Pointer)
		atomic.StorePointer(shard.l0s, unsafe.Pointer(new(l0Tables)))
		newShards[i] = shard
		shard.commitTS = commitTS
	}
	l0s := oldShard.loadL0Tables()
	for _, l0 := range l0s.tables {
		idx := sdb.getL0SplitIndex(l0, oldShard.splitKeys)
		nShard := newShards[idx]
		nL0s := nShard.loadL0Tables()
		nL0s.tables = append(nL0s.tables, l0)
	}
	for cf, scf := range oldShard.cfs {
		for l := 1; l <= ShardMaxLevel; l++ {
			level := scf.getLevelHandler(l)
			for _, t := range level.tables {
				sdb.insertTableToNewShard(t, cf, level.level, newShards, oldShard.splitKeys)
			}
		}
	}
	for _, nShard := range newShards {
		sdb.shardMap.Store(nShard.ID, nShard)
		mem := sdb.switchMemTable(nShard, nShard.allocCommitTS())
		sdb.scheduleFlushTask(nShard, mem)
	}
	log.S().Infof("shard %d split to %s", oldShard.ID, newShardsProps)
	return
}

func (sdb *DB) getL0SplitIndex(l0 *sstable.L0Table, splitKeys [][]byte) int {
	for i := 0; i < sdb.numCFs; i++ {
		cfTbl := l0.GetCF(i)
		if cfTbl != nil {
			return getSplitShardIndex(splitKeys, cfTbl.Smallest())
		}
	}
	return 0
}

func (sdb *DB) insertTableToNewShard(t table.Table, cf, level int, shards []*Shard, splitKeys [][]byte) {
	idx := getSplitShardIndex(splitKeys, t.Smallest())
	shard := shards[idx]
	if !shard.OverlapKey(t.Smallest()) || !shard.OverlapKey(t.Biggest()) {
		log.S().Fatalf("shard:%d:%d start:%x end:%x insert table %d smallest:%x biggest:%x",
			shard.ID, shard.Ver, shard.Start, shard.End, t.ID(), t.Smallest(), t.Biggest())
	}
	sCF := shard.cfs[cf]
	handler := sCF.getLevelHandler(level)
	handler.totalSize += t.Size()
	handler.tables = append(handler.tables, t)
}

func getSplitShardIndex(splitKeys [][]byte, key []byte) int {
	for i := 0; i < len(splitKeys); i++ {
		if bytes.Compare(key, splitKeys[i]) < 0 {
			return i
		}
	}
	return len(splitKeys)
}
