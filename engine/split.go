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
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ngaut/unistore/engine/fileutil"
	"github.com/ngaut/unistore/engine/table"
	"github.com/ngaut/unistore/engine/table/memtable"
	"github.com/ngaut/unistore/engine/table/sstable"
	"github.com/ngaut/unistore/enginepb"
	"github.com/ngaut/unistore/scheduler"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// PreSplit sets the split keys, then all new entries are written to separated mem-tables.
func (en *Engine) PreSplit(cs *enginepb.ChangeSet) error {
	guard := en.resourceMgr.Acquire()
	defer guard.Done()
	shard := en.GetShard(cs.ShardID)
	if shard == nil {
		return ErrShardNotFound
	}
	if shard.Ver != cs.ShardVer {
		log.Info("shard not match", zap.Uint64("current", shard.Ver), zap.Uint64("request", cs.ShardVer))
		return ErrShardNotMatch
	}
	if !shard.setSplitKeys(cs.PreSplit.Keys) {
		return ErrPreSplitWrongStage
	}
	commitTS := shard.loadCommitTS()
	memTbl := en.switchMemTable(shard, commitTS)
	en.scheduleFlushTask(shard, memTbl)
	return nil
}

// SplitShardFiles splits the files that overlaps the split keys.
func (en *Engine) SplitShardFiles(shardID, ver uint64) (*enginepb.ChangeSet, error) {
	guard := en.resourceMgr.Acquire()
	defer guard.Done()
	shard := en.GetShard(shardID)
	if shard == nil {
		log.S().Warnf("shard %d:%d shard not found", shard.ID, shard.Ver)
		return nil, ErrShardNotFound
	}
	if shard.Ver != ver {
		log.S().Warnf("shard %d:%d shard not match current %d, request %d", shard.ID, shard.Ver, shard.Ver, ver)
		return nil, ErrShardNotMatch
	}
	if !shard.isSplitting() {
		log.S().Warnf("wrong splitting stage for split files %s", shard.GetSplitStage())
		return nil, ErrSplitFilesWrongStage
	}
	change := newChangeSet(shard)
	change.SplitFiles = &enginepb.SplitFiles{}
	change.Stage = enginepb.SplitStage_SPLIT_FILE_DONE
	shard.lock.Lock()
	defer shard.lock.Unlock()
	keys := shard.splitKeys
	en.waitForPreSplitFlushState(shard)
	err := en.splitShardL0Tables(shard, change.SplitFiles)
	if err != nil {
		return nil, err
	}
	for cf := 0; cf < en.numCFs; cf++ {
		for lvl := 1; lvl <= en.getCFMaxLevel(cf); lvl++ {
			if err = en.splitTables(shard, cf, lvl, keys, change.SplitFiles); err != nil {
				return nil, err
			}
		}
	}
	return change, nil
}

func (en *Engine) waitForPreSplitFlushState(shard *Shard) {
	for {
		switch shard.GetSplitStage() {
		case enginepb.SplitStage_PRE_SPLIT_FLUSH_DONE, enginepb.SplitStage_SPLIT_FILE_DONE:
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (en *Engine) splitShardL0Tables(shard *Shard, splitFiles *enginepb.SplitFiles) error {
	l0s := shard.loadL0Tables()
	for i := 0; i < len(l0s.tables); i++ {
		l0Tbl := l0s.tables[i]
		if !en.needSplitL0(shard, l0Tbl) {
			continue
		}
		newL0s, err := en.splitShardL0Table(shard, l0Tbl)
		if err != nil {
			return err
		}
		splitFiles.L0Creates = append(splitFiles.L0Creates, newL0s...)
		splitFiles.TableDeletes = append(splitFiles.TableDeletes, l0Tbl.ID())
	}
	return nil
}

func (en *Engine) splitShardL0Table(shard *Shard, l0 *sstable.L0Table) ([]*enginepb.L0Create, error) {
	iters := make([]table.Iterator, en.numCFs)
	for cf := 0; cf < en.numCFs; cf++ {
		iters[cf] = l0.NewIterator(cf, false)
		if iters[cf] != nil {
			it := iters[cf]
			defer it.Close()
			it.Rewind()
		}
	}
	var newL0s []*enginepb.L0Create
	var bt *scheduler.BatchTasks
	if en.s3c != nil {
		bt = scheduler.NewBatchTasks()
	}
	for _, key := range shard.splitKeys {
		result, err := en.buildShardL0BeforeKey(iters, key, l0.CommitTS())
		if err != nil {
			return nil, err
		}
		if result != nil {
			if en.s3c != nil {
				bt.AppendTask(func() error {
					return putSSTBuildResultToS3(en.s3c, result)
				})
			}
			newL0 := newL0CreateByResult(result)
			newL0s = append(newL0s, newL0)
		}
	}
	result, err := en.buildShardL0BeforeKey(iters, shard.End, l0.CommitTS())
	if err != nil {
		return nil, err
	}
	if result != nil {
		if en.s3c != nil {
			bt.AppendTask(func() error {
				return putSSTBuildResultToS3(en.s3c, result)
			})
		}
		lastL0 := newL0CreateByResult(result)
		newL0s = append(newL0s, lastL0)
	}
	if en.s3c != nil {
		if err := en.s3c.BatchSchedule(bt); err != nil {
			return nil, err
		}
	}
	return newL0s, nil
}

func (en *Engine) needSplitL0(shard *Shard, l0 *sstable.L0Table) bool {
	for _, splitKey := range shard.splitKeys {
		for cf := 0; cf < en.numCFs; cf++ {
			tbl := l0.GetCF(cf)
			if tbl != nil {
				if bytes.Compare(tbl.Smallest(), splitKey) < 0 && bytes.Compare(tbl.Biggest(), splitKey) >= 0 {
					return true
				}
			}
		}
	}
	return false
}

func (en *Engine) buildShardL0BeforeKey(iters []table.Iterator, endKey []byte, commitTS uint64) (*sstable.BuildResult, error) {
	fid, err := en.idAlloc.AllocID(1)
	if err != nil {
		return nil, err
	}
	builder := sstable.NewL0Builder(en.numCFs, fid, en.opt.TableBuilderOptions, commitTS)
	var hasData bool
	for cf := 0; cf < en.numCFs; cf++ {
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
	smallset, biggest := builder.SmallestAndBiggest()
	fd, err := en.createL0File(fid)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	writer := fileutil.NewBufferedWriter(fd, en.opt.TableBuilderOptions.WriteBufferSize, nil)
	_, err = writer.Write(shardL0Data)
	if err != nil {
		return nil, err
	}
	err = writer.Finish()
	if err != nil {
		return nil, err
	}
	result := &sstable.BuildResult{
		ID:       fid,
		FileData: shardL0Data,
		Smallest: smallset,
		Biggest:  biggest,
	}
	return result, nil
}

func (en *Engine) splitTables(shard *Shard, cf int, level int, keys [][]byte, splitFiles *enginepb.SplitFiles) error {
	scf := shard.cfs[cf]
	oldHandler := scf.getLevelHandler(level)
	oldTables := oldHandler.tables
	toDeleteIDs := make(map[uint64]struct{})
	var relatedKeys [][]byte
	var bt *scheduler.BatchTasks
	if en.s3c != nil {
		bt = scheduler.NewBatchTasks()
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
		relatedKeys = append(relatedKeys, GlobalShardEndKey)
		itr := tbl.NewIterator(false)
		defer itr.Close()
		itr.Rewind()
		for _, relatedKey := range relatedKeys {
			result, err := en.buildTableBeforeKey(itr, relatedKey, en.opt.TableBuilderOptions)
			if err != nil {
				return err
			}
			if result != nil {
				if en.s3c != nil {
					bt.AppendTask(func() error {
						return putSSTBuildResultToS3(en.s3c, result)
					})
				}
				splitFiles.TableCreates = append(splitFiles.TableCreates, newTableCreateByResult(result, cf, level))
			}
		}
		splitFiles.TableDeletes = append(splitFiles.TableDeletes, tbl.ID())
	}
	if en.s3c != nil {
		if err := en.s3c.BatchSchedule(bt); err != nil {
			return err
		}
	}
	return nil
}

func (en *Engine) buildTableBeforeKey(itr table.Iterator, key []byte, opt sstable.TableBuilderOptions) (*sstable.BuildResult, error) {
	id, err := en.idAlloc.AllocID(1)
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
	result, err1 := b.Finish(id, bytes.NewBuffer(make([]byte, 0, b.EstimateSize())))
	if err1 != nil {
		return nil, err1
	}
	return result, nil
}

// FinishSplit finishes the Split process on a Shard in PreSplitStage.
// This is done after preSplit is done, so we don't need to acquire any lock, just atomic CAS will do.
func (en *Engine) FinishSplit(changeSet *enginepb.ChangeSet) (err error) {
	oldShard := en.GetShard(changeSet.ShardID)
	if oldShard.Ver != changeSet.ShardVer {
		return ErrShardNotMatch
	}
	if oldShard.GetSplitStage() != enginepb.SplitStage_SPLIT_FILE_DONE {
		return ErrFinishSplitWrongStage
	}
	split := changeSet.Split
	if len(split.NewShards) != len(oldShard.splittingMemTbls) {
		return fmt.Errorf("newShardsProps length %d is not equals to splittingMemTbls length %d", len(split.NewShards), len(oldShard.splittingMemTbls))
	}
	en.buildSplitShards(oldShard, split.NewShards, changeSet.Sequence)
	return nil
}

func (en *Engine) buildSplitShards(oldShard *Shard, newShardsProps []*enginepb.Properties, seq uint64) (newShards []*Shard) {
	newShards = make([]*Shard, len(oldShard.splittingMemTbls))
	newVer := oldShard.Ver + uint64(len(newShardsProps)) - 1
	for i := range oldShard.splittingMemTbls {
		startKey, endKey := getSplittingStartEnd(oldShard.Start, oldShard.End, oldShard.splitKeys, i)
		shard := newShard(newShardsProps[i], newVer, startKey, endKey, &en.opt)
		if shard.ID != oldShard.ID {
			shard.SetPassive(true)
		} else {
			shard.SetPassive(oldShard.IsPassive())
		}
		shard.memTbls = new(unsafe.Pointer)
		atomic.StorePointer(shard.memTbls, unsafe.Pointer(&memTables{tables: []*memtable.Table{oldShard.loadSplittingMemTable(i)}}))
		shard.l0s = new(unsafe.Pointer)
		atomic.StorePointer(shard.l0s, unsafe.Pointer(new(l0Tables)))
		newShards[i] = shard
		shard.baseTS = oldShard.baseTS + oldShard.sequence
		if shard.ID == oldShard.ID {
			shard.sequence = seq
			// derived shard need larger mem-table size.
			memSize := boundedMemSize(en.opt.BaseSize / 2)
			shard.properties.set(MemTableSizeKey, sstable.AppendU64(nil, uint64(memSize)))
			shard.maxMemTableSize = memSize
		} else {
			shard.sequence = 1
		}
	}
	l0s := oldShard.loadL0Tables()
	for _, l0 := range l0s.tables {
		idx := getSplitShardIndex(oldShard.splitKeys, l0.Smallest())
		nShard := newShards[idx]
		nL0s := nShard.loadL0Tables()
		nL0s.tables = append(nL0s.tables, l0)
	}
	for cf, scf := range oldShard.cfs {
		for l := 1; l <= len(scf.levels); l++ {
			level := scf.getLevelHandler(l)
			for _, t := range level.tables {
				en.insertTableToNewShard(t, cf, level.level, newShards, oldShard.splitKeys)
			}
		}
	}
	for _, nShard := range newShards {
		nShard.refreshEstimatedSize()
		en.shardMap.Store(nShard.ID, nShard)
		commitTS := nShard.loadCommitTS()
		mem := en.switchMemTable(nShard, commitTS)
		en.scheduleFlushTask(nShard, mem)
		log.S().Infof("new shard %d:%d mem-size %d props:%s commitTS: %d",
			nShard.ID, nShard.Ver, mem.Size(), nShard.properties, commitTS)
	}
	return
}

func (en *Engine) insertTableToNewShard(t table.Table, cf, level int, shards []*Shard, splitKeys [][]byte) {
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
