package sdb

import (
	"bytes"
	"fmt"
	"github.com/ngaut/unistore/sdb/table/memtable"
	"github.com/ngaut/unistore/sdbpb"
	"math"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ngaut/unistore/sdb/table"
	"github.com/ngaut/unistore/sdb/table/sstable"
	"github.com/pingcap/badger/fileutil"
	"github.com/pingcap/badger/options"
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
	notify := make(chan error, 1)
	task := &preSplitTask{
		shard: shard,
		keys:  keys,
	}
	sdb.writeCh <- engineTask{preSplitTask: task, notify: notify}
	return <-notify
}

// executePreSplitTask is executed in the write thread.
func (sdb *DB) executePreSplitTask(eTask engineTask) {
	task := eTask.preSplitTask
	shard := task.shard
	if !shard.setSplitKeys(task.keys) {
		eTask.notify <- errors.New("failed to set split keys")
		return
	}
	change := newChangeSet(shard)
	change.State = sdbpb.SplitState_PRE_SPLIT
	change.PreSplit = &sdbpb.PreSplit{
		Keys:     task.keys,
		MemProps: shard.properties.toPB(shard.ID),
	}
	err := sdb.manifest.writeChangeSet(change)
	if err != nil {
		eTask.notify <- err
		return
	}
	commitTS := sdb.orc.readTs()
	memTbl := sdb.switchMemTable(shard, 0, commitTS)
	sdb.scheduleFlushTask(shard, memTbl, commitTS, true)
	eTask.notify <- nil
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
		log.S().Infof("wrong splitting state %s", shard.GetSplitState())
		return nil, errShardWrongSplittingState
	}
	change := newChangeSet(shard)
	change.SplitFiles = &sdbpb.SplitFiles{}
	change.State = sdbpb.SplitState_SPLIT_FILE_DONE
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
		switch shard.GetSplitState() {
		case sdbpb.SplitState_PRE_SPLIT_FLUSH_DONE, sdbpb.SplitState_SPLIT_FILE_DONE:
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
		splitFiles.TableDeletes = append(splitFiles.TableDeletes, l0Tbl.fid)
	}
	return nil
}

func (sdb *DB) splitShardL0Table(shard *Shard, l0 *l0Table) ([]*sdbpb.L0Create, error) {
	iters := make([]y.Iterator, sdb.numCFs)
	for cf := 0; cf < sdb.numCFs; cf++ {
		iters[cf] = l0.newIterator(cf, false)
		if iters[cf] != nil {
			it := iters[cf]
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
			}
			it.Rewind()
		}
	}
	var newL0s []*sdbpb.L0Create
	for i, key := range shard.splitKeys {
		startKey := shard.Start
		if i != 0 {
			startKey = shard.splitKeys[i-1]
		}
		newL0, err := sdb.buildShardL0BeforeKey(iters, startKey, key, l0.commitTS)
		if err != nil {
			return nil, err
		}
		if newL0 != nil {
			newL0s = append(newL0s, newL0)
		}
	}
	lastL0, err := sdb.buildShardL0BeforeKey(iters, shard.splitKeys[len(shard.splitKeys)-1], shard.End, l0.commitTS)
	if err != nil {
		return nil, err
	}
	if lastL0 != nil {
		newL0s = append(newL0s, lastL0)
	}
	return newL0s, nil
}

func (sdb *DB) buildShardL0BeforeKey(iters []y.Iterator, startKey, endKey []byte, commitTS uint64) (*sdbpb.L0Create, error) {
	builder := newL0Builder(sdb.numCFs, sdb.opt.TableBuilderOptions, commitTS)
	var hasData bool
	for cf := 0; cf < sdb.numCFs; cf++ {
		iter := iters[cf]
		if iter == nil {
			continue
		}
		for ; iter.Valid(); y.NextAllVersion(iter) {
			if bytes.Compare(iter.Key().UserKey, endKey) >= 0 {
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
	fid := sdb.idAlloc.AllocID()
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
		Smallest: y.KeyWithTs(startKey, math.MaxUint64),
		Biggest:  y.KeyWithTs(endKey, 0),
	}
	if sdb.s3c != nil {
		err = putSSTBuildResultToS3(sdb.s3c, result)
		if err != nil {
			return nil, err
		}
	}
	return newL0CreateByResult(result, nil), nil
}

func (sdb *DB) splitTables(shard *Shard, cf int, level int, keys [][]byte, splitFiles *sdbpb.SplitFiles) error {
	scf := shard.cfs[cf]
	oldHandler := scf.getLevelHandler(level)
	oldTables := oldHandler.tables
	toDeleteIDs := make(map[uint64]struct{})
	var relatedKeys [][]byte
	for _, tbl := range oldTables {
		relatedKeys = relatedKeys[:0]
		for _, key := range keys {
			if bytes.Compare(tbl.Smallest().UserKey, key) < 0 &&
				bytes.Compare(key, tbl.Biggest().UserKey) <= 0 {
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
				splitFiles.TableCreates = append(splitFiles.TableCreates, newTableCreateByResult(result, cf, level))
			}
		}
		splitFiles.TableDeletes = append(splitFiles.TableDeletes, tbl.ID())
	}
	return nil
}

func (sdb *DB) buildTableBeforeKey(itr y.Iterator, key []byte, level int, opt options.TableBuilderOptions) (*sstable.BuildResult, error) {
	filename := sstable.NewFilename(sdb.idAlloc.AllocID(), sdb.opt.Dir)
	fd, err := y.OpenSyncedFile(filename, false)
	if err != nil {
		return nil, err
	}
	b := sstable.NewTableBuilder(fd, nil, level, opt)
	for itr.Valid() {
		if len(key) > 0 && bytes.Compare(itr.Key().UserKey, key) >= 0 {
			break
		}
		err = b.Add(itr.Key(), itr.Value())
		if err != nil {
			return nil, err
		}
		y.NextAllVersion(itr)
	}
	if b.Empty() {
		return nil, nil
	}
	result, err1 := b.Finish()
	if err1 != nil {
		return nil, err1
	}
	if sdb.s3c != nil {
		err = putSSTBuildResultToS3(sdb.s3c, result)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// FinishSplit finishes the Split process on a Shard in PreSplitState.
// This is done after preSplit is done, so we don't need to acquire any lock, just atomic CAS will do.
func (sdb *DB) FinishSplit(oldShardID, ver uint64, newShardsProps []*sdbpb.Properties) (newShards []*Shard, err error) {
	oldShard := sdb.GetShard(oldShardID)
	if oldShard.Ver != ver {
		return nil, errShardNotMatch
	}
	if !oldShard.isSplitting() {
		return nil, errors.New("shard is not in splitting state")
	}
	if len(newShardsProps) != len(oldShard.splittingMemTbls) {
		return nil, fmt.Errorf("newShardsProps length %d is not equals to splittingMemTbls length %d", len(newShardsProps), len(oldShard.splittingMemTbls))
	}
	notify := make(chan error, 1)
	task := &finishSplitTask{
		shard:    oldShard,
		newProps: newShardsProps,
	}
	sdb.writeCh <- engineTask{finishSplitTask: task, notify: notify}
	err = <-notify
	if err != nil {
		return nil, err
	}
	return task.newShards, nil
}

// executeFinishSplitTask write the last entry and finish the WAL.
// It is executed in the write thread.
func (sdb *DB) executeFinishSplitTask(eTask engineTask) {
	task := eTask.finishSplitTask
	oldShard := task.shard
	latest := sdb.GetShard(oldShard.ID)
	if latest.Ver != oldShard.Ver {
		eTask.notify <- errShardNotMatch
		return
	}
	changeSet := newChangeSet(task.shard)
	changeSet.Split = &sdbpb.Split{
		NewShards: task.newProps,
		Keys:      oldShard.splitKeys,
		MemProps:  oldShard.properties.toPB(oldShard.ID),
	}
	err := sdb.manifest.writeChangeSet(changeSet)
	if err != nil {
		eTask.notify <- err
		return
	}
	newShards := sdb.buildSplitShards(oldShard, task.newProps)
	task.newShards = newShards
	eTask.notify <- nil
	return
}

func (sdb *DB) buildSplitShards(oldShard *Shard, newShardsProps []*sdbpb.Properties) (newShards []*Shard) {
	newShards = make([]*Shard, len(oldShard.splittingMemTbls))
	newVer := oldShard.Ver + uint64(len(newShardsProps)) - 1
	for i := range oldShard.splittingMemTbls {
		startKey, endKey := getSplittingStartEnd(oldShard.Start, oldShard.End, oldShard.splitKeys, i)
		shard := newShard(newShardsProps[i], newVer, startKey, endKey, sdb.opt, sdb.metrics)
		if oldShard.IsPassive() {
			shard.SetPassive(true)
		}
		log.S().Infof("new shard %d:%d state %s", shard.ID, shard.Ver, shard.GetSplitState())
		shard.memTbls = new(unsafe.Pointer)
		atomic.StorePointer(shard.memTbls, unsafe.Pointer(&memTables{tables: []*memtable.CFTable{oldShard.loadSplittingMemTable(i)}}))
		shard.l0s = new(unsafe.Pointer)
		atomic.StorePointer(shard.l0s, unsafe.Pointer(new(l0Tables)))
		newShards[i] = shard
	}
	l0s := oldShard.loadL0Tables()
	for _, l0 := range l0s.tables {
		idx := l0.getSplitIndex(oldShard.splitKeys)
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
	}
	log.S().Infof("shard %d split to %s", oldShard.ID, newShardsProps)
	return
}

func (sdb *DB) insertTableToNewShard(t table.Table, cf, level int, shards []*Shard, splitKeys [][]byte) {
	idx := getSplitShardIndex(splitKeys, t.Smallest().UserKey)
	shard := shards[idx]
	y.Assert(shard.OverlapKey(t.Smallest().UserKey))
	y.Assert(shard.OverlapKey(t.Biggest().UserKey))
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
