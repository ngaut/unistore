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
	"encoding/binary"
	"fmt"
	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/engine/epoch"
	"github.com/ngaut/unistore/engine/table"
	"github.com/ngaut/unistore/engine/table/memtable"
	"github.com/ngaut/unistore/enginepb"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type Shard struct {
	ID    uint64
	Ver   uint64
	Start []byte
	End   []byte
	cfs   []*shardCF
	lock  sync.Mutex
	opt   *Options

	memTbls *unsafe.Pointer
	l0s     *unsafe.Pointer
	flushCh chan *flushTask

	// split stage transition: initial(0) -> pre-split (1) -> pre-split-flush-done (2) -> split-file-done (3)
	splitStage       int32
	splitKeys        [][]byte
	splittingMemTbls []unsafe.Pointer

	// If the shard is passive, flush mem table and do compaction will ignore this shard.
	passive int32

	properties      *properties
	compacting      int32
	initialFlushed  int32
	lastSwitchTime  time.Time
	maxMemTableSize int64

	skippedFlushes   []*flushTask
	skippedFlushLock sync.Mutex

	commitTS uint64

	estimatedSize int64
}

const (
	MemTableSizeKey = "_mem_table_size"
)

func newShard(props *enginepb.Properties, ver uint64, start, end []byte, opt *Options) *Shard {
	shard := &Shard{
		ID:              props.ShardID,
		Ver:             ver,
		Start:           start,
		End:             end,
		opt:             opt,
		cfs:             make([]*shardCF, len(opt.CFs)),
		properties:      newProperties().applyPB(props),
		maxMemTableSize: opt.BaseSize / 4,
		lastSwitchTime:  time.Now(),
	}
	val, ok := shard.properties.get(MemTableSizeKey)
	if ok {
		shard.setMaxMemTableSize(int64(binary.LittleEndian.Uint64(val)))
	}
	shard.memTbls = new(unsafe.Pointer)
	atomic.StorePointer(shard.memTbls, unsafe.Pointer(&memTables{}))
	shard.l0s = new(unsafe.Pointer)
	atomic.StorePointer(shard.l0s, unsafe.Pointer(&l0Tables{}))
	for i := 0; i < len(opt.CFs); i++ {
		sCF := &shardCF{
			levels: make([]unsafe.Pointer, opt.CFs[i].MaxLevels),
		}
		for j := 1; j <= len(sCF.levels); j++ {
			sCF.casLevelHandler(j, nil, newLevelHandler(opt.NumLevelZeroTablesStall, j))
		}
		shard.cfs[i] = sCF
	}
	return shard
}

func newShardForLoading(shardInfo *ShardMeta, opt *Options) *Shard {
	shard := newShard(shardInfo.properties.toPB(shardInfo.ID), shardInfo.Ver, shardInfo.Start, shardInfo.End, opt)
	if shardInfo.preSplit != nil && shardInfo.splitStage >= enginepb.SplitStage_PRE_SPLIT_FLUSH_DONE {
		// We don't set split keys if the split stage has not reached enginepb.SplitStage_PRE_SPLIT_FLUSH_DONE
		// because some the recover data should be execute before split key is set.
		shard.setSplitKeys(shardInfo.preSplit.Keys)
	}
	shard.setSplitStage(shardInfo.splitStage)
	shard.setInitialFlushed()
	shard.SetPassive(true)
	shard.commitTS = shardInfo.commitTS
	return shard
}

func newShardForIngest(changeSet *enginepb.ChangeSet, opt *Options) *Shard {
	shardSnap := changeSet.Snapshot
	shard := newShard(shardSnap.Properties, changeSet.ShardVer, shardSnap.Start, shardSnap.End, opt)
	if changeSet.PreSplit != nil {
		log.S().Infof("shard %d:%d set pre-split keys by ingest", changeSet.ShardID, changeSet.ShardVer)
		shard.setSplitKeys(changeSet.PreSplit.Keys)
	}
	shard.setSplitStage(changeSet.Stage)
	shard.setInitialFlushed()
	shard.commitTS = shardSnap.CommitTS
	log.S().Infof("ingest shard %d:%d maxMemTblSize %d, commitTS %d",
		changeSet.ShardID, changeSet.ShardVer, shard.getMaxMemTableSize(), shard.commitTS)
	return shard
}

func (s *Shard) tableIDs() []uint64 {
	var ids []uint64
	l0s := s.loadL0Tables()
	for _, tbl := range l0s.tables {
		ids = append(ids, tbl.ID())
	}
	s.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		for _, tbl := range level.tables {
			ids = append(ids, tbl.ID())
		}
		return false
	})
	return ids
}

func (s *Shard) IsPassive() bool {
	return atomic.LoadInt32(&s.passive) == 1
}

func (s *Shard) SetPassive(passive bool) {
	v := int32(0)
	if passive {
		v = 1
	}
	atomic.StoreInt32(&s.passive, v)
}

func (s *Shard) isSplitting() bool {
	return atomic.LoadInt32(&s.splitStage) >= int32(enginepb.SplitStage_PRE_SPLIT)
}

func (s *Shard) GetEstimatedSize() int64 {
	return atomic.LoadInt64(&s.estimatedSize)
}

func (s *Shard) refreshEstimatedSize() {
	l0Tables := s.loadL0Tables()
	L0TablesSize := int64(0)
	for _, t := range l0Tables.tables {
		L0TablesSize += t.Size()
	}
	CFsSize := int64(0)
	s.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		CFsSize += level.totalSize
		return false
	})
	atomic.StoreInt64(&s.estimatedSize, L0TablesSize+CFsSize)
}

func (s *Shard) getMaxMemTableSize() int64 {
	return atomic.LoadInt64(&s.maxMemTableSize)
}

func (s *Shard) setMaxMemTableSize(size int64) {
	atomic.StoreInt64(&s.maxMemTableSize, size)
}

func (s *Shard) setSplitKeys(keys [][]byte) bool {
	if s.GetSplitStage() == enginepb.SplitStage_INITIAL {
		s.splitKeys = keys
		s.splittingMemTbls = make([]unsafe.Pointer, len(keys)+1)
		for i := range s.splittingMemTbls {
			atomic.StorePointer(&s.splittingMemTbls[i], unsafe.Pointer(memtable.NewCFTable(len(s.cfs))))
		}
		s.setSplitStage(enginepb.SplitStage_PRE_SPLIT)
		log.S().Debugf("shard %d:%d pre-split", s.ID, s.Ver)
		return true
	}
	log.S().Warnf("shard %d:%d failed to set split key got stage %s", s.ID, s.Ver, s.GetSplitStage())
	return false
}

func (s *Shard) foreachLevel(f func(cf int, level *levelHandler) (stop bool)) {
	for cf, scf := range s.cfs {
		for i := 1; i <= len(scf.levels); i++ {
			l := scf.getLevelHandler(i)
			if stop := f(cf, l); stop {
				return
			}
		}
	}
}

func (s *Shard) getSplittingIndex(key []byte) int {
	var i int
	for ; i < len(s.splitKeys); i++ {
		if bytes.Compare(key, s.splitKeys[i]) < 0 {
			break
		}
	}
	return i
}

func (s *Shard) loadSplittingMemTable(i int) *memtable.Table {
	return (*memtable.Table)(atomic.LoadPointer(&s.splittingMemTbls[i]))
}

func (s *Shard) loadMemTables() *memTables {
	return (*memTables)(atomic.LoadPointer(s.memTbls))
}

func (s *Shard) loadWritableMemTable() *memtable.Table {
	tbls := s.loadMemTables()
	if len(tbls.tables) > 0 {
		return tbls.tables[0]
	}
	return nil
}

func (s *Shard) loadL0Tables() *l0Tables {
	return (*l0Tables)(atomic.LoadPointer(s.l0s))
}

func (s *Shard) getSuggestSplitKeys(targetSize int64) [][]byte {
	estimatedSize := s.GetEstimatedSize()
	if estimatedSize < targetSize {
		return nil
	}
	if splitKey, ok := s.getSequentialWriteSplitKey(targetSize); ok {
		return [][]byte{splitKey}
	}
	var maxLevel *levelHandler
	s.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		if maxLevel == nil {
			maxLevel = level
		}
		if maxLevel.totalSize < level.totalSize {
			maxLevel = level
		}
		return false
	})
	levelTargetSize := int64(float64(targetSize) * (float64(maxLevel.totalSize) / float64(estimatedSize)))
	var keys [][]byte
	var currentSize int64
	for i, tbl := range maxLevel.tables {
		currentSize += tbl.Size()
		if i != 0 && currentSize > levelTargetSize {
			keys = append(keys, tbl.Smallest())
			currentSize = 0
		}
	}
	return keys
}

func (s *Shard) getSequentialWriteSplitKey(targetSize int64) ([]byte, bool) {
	l0s := s.loadL0Tables()
	if l0s.totalSize() < targetSize/8 {
		// If L0 table's size is not large enough, we don't consider it's a sequential write.
		return nil, false
	}
	if len(l0s.tables) == 1 {
		return nil, false
	}
	newTbl, oldTbl := l0s.tables[0].GetCF(0), l0s.tables[1].GetCF(0)
	if newTbl == nil || oldTbl == nil {
		return nil, false
	}
	blockIdx := newTbl.SeekBlock(oldTbl.Biggest())
	if blockIdx > newTbl.NumBlocks()/2 {
		return nil, false
	}
	// More than half of the new table's key is larger than the previous table's biggest key.
	// We use the previous table's biggest key as the split key.
	return append(y.Copy(oldTbl.Biggest()), 0), true
}

func (s *Shard) GetPreSplitKeys() [][]byte {
	if s.GetSplitStage() == enginepb.SplitStage_INITIAL {
		return nil
	}
	return s.splitKeys
}

func getSplittingStartEnd(oldStart, oldEnd []byte, splitKeys [][]byte, i int) (startKey, endKey []byte) {
	if i != 0 {
		startKey = splitKeys[i-1]
	} else {
		startKey = oldStart
	}
	if i == len(splitKeys) {
		endKey = oldEnd
	} else {
		endKey = splitKeys[i]
	}
	return
}

func (s *Shard) OverlapRange(startKey, endKey []byte) bool {
	return bytes.Compare(s.Start, endKey) < 0 && bytes.Compare(startKey, s.End) < 0
}

func (s *Shard) OverlapKey(key []byte) bool {
	return bytes.Compare(s.Start, key) <= 0 && bytes.Compare(key, s.End) < 0
}

func (s *Shard) GetSplitStage() enginepb.SplitStage {
	return enginepb.SplitStage(atomic.LoadInt32(&s.splitStage))
}

func (s *Shard) setSplitStage(stage enginepb.SplitStage) {
	oldStage := s.GetSplitStage()
	if int32(oldStage) == int32(stage) {
		return
	}
	if int32(oldStage) > int32(stage) {
		return
	}
	atomic.CompareAndSwapInt32(&s.splitStage, int32(oldStage), int32(stage))
	log.S().Infof("shard %d:%d set split stage %s", s.ID, s.Ver, stage)
}

func (s *Shard) RecoverGetProperty(key string) ([]byte, bool) {
	return s.properties.get(key)
}

func (s *Shard) RecoverSetProperty(key string, val []byte) {
	s.properties.set(key, val)
}

func (s *Shard) markCompacting(b bool) {
	if b {
		atomic.StoreInt32(&s.compacting, 1)
	} else {
		atomic.StoreInt32(&s.compacting, 0)
	}
}

func (s *Shard) isCompacting() bool {
	return atomic.LoadInt32(&s.compacting) == 1
}

func (s *Shard) IsInitialFlushed() bool {
	return atomic.LoadInt32(&s.initialFlushed) == 1
}

func (s *Shard) setInitialFlushed() {
	atomic.StoreInt32(&s.initialFlushed, 1)
}

const (
	maxMemSizeUpperLimit = 128 * config.MB
	maxMemSizeLowerLimit = 4 * config.MB
)

func (s *Shard) nextMemTableSize(writableMemTableSize int64, lastSwitchTime time.Time) int64 {
	dur := time.Since(lastSwitchTime)
	timeInMs := int64(dur/time.Millisecond) + 1 // + 1 to avoid divide by zero.
	bytesPerSec := writableMemTableSize * 1000 / timeInMs
	nextMemSize := bytesPerSec * int64(s.opt.MaxMemTableSizeFactor)
	return boundedMemSize(nextMemSize)
}

func boundedMemSize(size int64) int64 {
	if size > maxMemSizeUpperLimit {
		size = maxMemSizeUpperLimit
	}
	if size < maxMemSizeLowerLimit {
		size = maxMemSizeLowerLimit
	}
	return size
}

func (s *Shard) allocCommitTS() uint64 {
	return atomic.AddUint64(&s.commitTS, 1)
}

type shardCF struct {
	levels []unsafe.Pointer
}

func (scf *shardCF) getLevelHandler(level int) *levelHandler {
	return (*levelHandler)(atomic.LoadPointer(&scf.levels[level-1]))
}

func (scf *shardCF) casLevelHandler(level int, oldH, newH *levelHandler) bool {
	return atomic.CompareAndSwapPointer(&scf.levels[level-1], unsafe.Pointer(oldH), unsafe.Pointer(newH))
}

func (scf *shardCF) setHasOverlapping(cd *CompactDef) {
	if cd.moveDown() {
		return
	}
	kr := getKeyRange(cd.Top)
	for lvl := cd.Level + 2; lvl <= len(scf.levels); lvl++ {
		lh := scf.getLevelHandler(lvl)
		left, right := lh.overlappingTables(kr)
		if right-left > 0 {
			cd.HasOverlap = true
			return
		}
	}
	return
}

type deletions struct {
	resources map[uint64]epoch.Resource
}

func (d *deletions) add(fid uint64, res epoch.Resource) {
	d.resources[fid] = res
}

func (d *deletions) remove(fid uint64) {
	delete(d.resources, fid)
}

func (d *deletions) collect() []epoch.Resource {
	resources := make([]epoch.Resource, 0, len(d.resources))
	for _, res := range d.resources {
		resources = append(resources, res)
	}
	return resources
}

type properties struct {
	m map[string][]byte
}

func newProperties() *properties {
	return &properties{m: map[string][]byte{}}
}

func (sp *properties) set(key string, val []byte) {
	y.Assert(len(key) < math.MaxUint16 && len(val) < math.MaxUint16)
	sp.m[key] = val
}

func (sp *properties) get(key string) ([]byte, bool) {
	v, ok := sp.m[key]
	return v, ok
}

func (sp *properties) toPB(shardID uint64) *enginepb.Properties {
	pbProps := &enginepb.Properties{
		ShardID: shardID,
		Keys:    make([]string, 0, len(sp.m)),
		Values:  make([][]byte, 0, len(sp.m)),
	}
	for k, v := range sp.m {
		pbProps.Keys = append(pbProps.Keys, k)
		pbProps.Values = append(pbProps.Values, v)
	}
	return pbProps
}

func (sp *properties) applyPB(pbProps *enginepb.Properties) *properties {
	if pbProps != nil {
		for i, key := range pbProps.Keys {
			sp.m[key] = pbProps.Values[i]
		}
	}
	return sp
}

func (sp *properties) String() string {
	var strs []string
	for k, v := range sp.m {
		strs = append(strs, fmt.Sprintf("%s:%x", k, v))
	}
	return strings.Join(strs, ",")
}

func GetShardProperty(key string, props *enginepb.Properties) ([]byte, bool) {
	for i, k := range props.Keys {
		if key == k {
			return props.Values[i], true
		}
	}
	return nil, false
}

func SetShardProperty(key string, value []byte, props *enginepb.Properties) {
	for i, k := range props.Keys {
		if key == k {
			props.Values[i] = value
			return
		}
	}
	props.Keys = append(props.Keys, key)
	props.Values = append(props.Values, value)
}

type levelHandler struct {
	// For level >= 1, tables are sorted by key ranges, which do not overlap.
	// For level 0, tables are sorted by time.
	// For level 0, newest table are at the back. Compact the oldest one first, which is at the front.
	tables    []table.Table
	totalSize int64

	// The following are initialized once and const.
	level      int
	numL0Stall int
}

func newLevelHandler(numL0Stall, level int) *levelHandler {
	return &levelHandler{
		level:      level,
		numL0Stall: numL0Stall,
	}
}

// overlappingTables returns the tables that intersect with key range. Returns a half-interval.
// This function should already have acquired a read lock, and this is so important the caller must
// pass an empty parameter declaring such.
func (s *levelHandler) overlappingTables(kr keyRange) (int, int) {
	return getTablesInRange(s.tables, kr.left, kr.right)
}

func getTablesInRange(tbls []table.Table, start, end []byte) (int, int) {
	left := sort.Search(len(tbls), func(i int) bool {
		return bytes.Compare(start, tbls[i].Biggest()) <= 0
	})
	right := sort.Search(len(tbls), func(i int) bool {
		return bytes.Compare(end, tbls[i].Smallest()) < 0
	})
	return left, right
}

// get returns value for a given key or the key after that. If not found, return nil.
func (s *levelHandler) get(key []byte, version, keyHash uint64) y.ValueStruct {
	return s.getInTable(key, version, keyHash, s.getTable(key))
}

func (s *levelHandler) getInTable(key []byte, version, keyHash uint64, table table.Table) y.ValueStruct {
	if table == nil {
		return y.ValueStruct{}
	}
	// TODO: error handling here
	result, err := table.Get(key, version, keyHash)
	if err != nil {
		log.Error("get data in table failed", zap.Error(err))
	}
	return result
}

func (s *levelHandler) getTable(key []byte) table.Table {
	// For level >= 1, we can do a binary search as key range does not overlap.
	idx := sort.Search(len(s.tables), func(i int) bool {
		return bytes.Compare(s.tables[i].Biggest(), key) >= 0
	})
	if idx >= len(s.tables) {
		// Given key is strictly > than every element we have.
		return nil
	}
	tbl := s.tables[idx]
	return tbl
}
