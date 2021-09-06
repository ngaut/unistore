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
	"fmt"
	"github.com/ngaut/unistore/enginepb"
	"sort"

	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
)

type ShardMeta struct {
	ID    uint64
	Ver   uint64
	Start []byte
	End   []byte
	Seq   uint64
	// fid -> level
	files map[uint64]*fileMeta
	// properties in ShardMeta is only updated on every mem-table flush, it's different than properties in the shard
	// which is updated on every write operation.
	properties *properties
	preSplit   *enginepb.PreSplit
	split      *enginepb.Split
	SplitStage enginepb.SplitStage
	commitTS   uint64
	parent     *ShardMeta
	baseTS     uint64
}

func NewShardMeta(cs *enginepb.ChangeSet) *ShardMeta {
	snap := cs.Snapshot
	shardMeta := &ShardMeta{
		ID:         cs.ShardID,
		Ver:        cs.ShardVer,
		Start:      snap.Start,
		End:        snap.End,
		Seq:        cs.Sequence,
		files:      map[uint64]*fileMeta{},
		properties: newProperties().applyPB(snap.Properties),
		SplitStage: cs.Stage,
		commitTS:   snap.CommitTS,
		baseTS:     snap.BaseTS,
	}
	if len(cs.Snapshot.SplitKeys) > 0 {
		shardMeta.preSplit = &enginepb.PreSplit{Keys: cs.Snapshot.SplitKeys}
	}
	for _, l0 := range cs.Snapshot.L0Creates {
		shardMeta.addFile(l0.ID, -1, 0, l0.Smallest, l0.Biggest)
	}
	for _, tbl := range cs.Snapshot.TableCreates {
		shardMeta.addFile(tbl.ID, tbl.CF, tbl.Level, tbl.Smallest, tbl.Biggest)
	}
	return shardMeta
}

func (si *ShardMeta) FileLevel(fid uint64) (int, bool) {
	fm, ok := si.files[fid]
	if ok {
		return int(fm.level), true
	}
	return 0, false
}

func (si *ShardMeta) GetProperty(key string) ([]byte, bool) {
	return si.properties.get(key)
}

func (si *ShardMeta) ApplyChangeSet(cs *enginepb.ChangeSet) {
	if si.IsDuplicatedChangeSet(cs) {
		return
	}
	if cs.Sequence > 0 {
		si.Seq = cs.Sequence
	}
	if cs.Flush != nil {
		si.ApplyFlush(cs)
		return
	}
	if cs.PreSplit != nil {
		si.ApplyPreSplit(cs)
		return
	}
	if cs.Compaction != nil {
		si.ApplyCompaction(cs)
		return
	}
	if cs.SplitFiles != nil {
		si.ApplySplitFiles(cs)
		return
	}
	panic(fmt.Sprintf("unexpected change set %s", cs))
}

func (si *ShardMeta) ApplyFlush(cs *enginepb.ChangeSet) {
	si.commitTS = cs.Flush.CommitTS
	si.parent = nil
	props := cs.Flush.Properties
	if props != nil {
		for i, key := range props.Keys {
			si.properties.set(key, props.Values[i])
		}
	}
	if si.SplitStage >= enginepb.SplitStage_PRE_SPLIT_FLUSH_DONE {
		log.S().Panicf("%d:%d this flush is not last mem-table after pre-split, commitTS %d ,seq %d", si.ID, si.Ver, cs.Flush.CommitTS, cs.Sequence)
	}
	if cs.Stage == enginepb.SplitStage_PRE_SPLIT_FLUSH_DONE {
		si.SplitStage = enginepb.SplitStage_PRE_SPLIT_FLUSH_DONE
	}
	if l0 := cs.Flush.L0Create; l0 != nil {
		si.addFile(l0.ID, -1, 0, l0.Smallest, l0.Biggest)
	}
}

func (si *ShardMeta) ApplyCompaction(cs *enginepb.ChangeSet) {
	if isMoveDown(cs.Compaction) {
		for _, create := range cs.Compaction.TableCreates {
			si.moveDownFile(create.ID, create.CF, create.Level)
		}
		return
	}
	for _, id := range cs.Compaction.TopDeletes {
		si.deleteFile(id)
	}
	for _, id := range cs.Compaction.BottomDeletes {
		si.deleteFile(id)
	}
	for _, create := range cs.Compaction.TableCreates {
		si.addFile(create.ID, create.CF, create.Level, create.Smallest, create.Biggest)
	}
}

func (si *ShardMeta) ApplyPreSplit(cs *enginepb.ChangeSet) {
	si.preSplit = cs.PreSplit
	si.SplitStage = enginepb.SplitStage_PRE_SPLIT
}

func (si *ShardMeta) ApplySplitFiles(cs *enginepb.ChangeSet) {
	for _, id := range cs.SplitFiles.TableDeletes {
		si.deleteFile(id)
	}
	for _, l0 := range cs.SplitFiles.L0Creates {
		si.addFile(l0.ID, -1, 0, l0.Smallest, l0.Biggest)
	}
	for _, tbl := range cs.SplitFiles.TableCreates {
		si.addFile(tbl.ID, tbl.CF, tbl.Level, tbl.Smallest, tbl.Biggest)
	}
	si.SplitStage = enginepb.SplitStage_SPLIT_FILE_DONE
}

func (si *ShardMeta) ApplySplit(cs *enginepb.ChangeSet) []*ShardMeta {
	old := si
	split := cs.Split
	newShards := make([]*ShardMeta, len(split.NewShards))
	newVer := old.Ver + uint64(len(newShards)) - 1
	for i := 0; i < len(split.NewShards); i++ {
		startKey, endKey := getSplittingStartEnd(old.Start, old.End, split.Keys, i)
		id := split.NewShards[i].ShardID
		shardInfo := &ShardMeta{
			ID:         id,
			Ver:        newVer,
			Start:      startKey,
			End:        endKey,
			files:      map[uint64]*fileMeta{},
			properties: newProperties().applyPB(split.NewShards[i]),
			parent:     old,
		}
		if id == old.ID {
			old.split = split
			shardInfo.baseTS = old.baseTS
			shardInfo.Seq = cs.Sequence
		} else {
			shardInfo.baseTS = old.baseTS + cs.Sequence
			shardInfo.Seq = 1
		}
		newShards[i] = shardInfo
	}
	for fid := range old.files {
		fileMeta := old.files[fid]
		shardIdx := getSplitShardIndex(split.Keys, fileMeta.smallest)
		newShards[shardIdx].files[fid] = fileMeta
	}
	return newShards
}

func (si *ShardMeta) moveDownFile(fid uint64, cf int32, level uint32) {
	log.S().Infof("%d:%d moveDown file %d to level %d", si.ID, si.Ver, fid, level)
	fm := si.files[fid]
	y.Assert(fm.level+1 == level)
	y.Assert(fm.cf == cf)
	fm.level = level
}

func (si *ShardMeta) deleteFile(fid uint64) {
	log.S().Infof("%d:%d delete file %d", si.ID, si.Ver, fid)
	delete(si.files, fid)
}

func (si *ShardMeta) addFile(id uint64, cf int32, level uint32, smallest, biggest []byte) {
	log.S().Infof("%d:%d add file %d", si.ID, si.Ver, id)
	si.files[id] = &fileMeta{
		cf:       cf,
		level:    level,
		smallest: smallest,
		biggest:  biggest,
	}
}

func (si *ShardMeta) ToChangeSet() *enginepb.ChangeSet {
	cs := &enginepb.ChangeSet{
		ShardID:  si.ID,
		ShardVer: si.Ver,
		Stage:    si.SplitStage,
		Sequence: si.Seq,
	}
	shardSnap := &enginepb.Snapshot{
		Start:      si.Start,
		End:        si.End,
		Properties: si.properties.toPB(si.ID),
		CommitTS:   si.commitTS,
		BaseTS:     si.baseTS,
	}
	if si.preSplit != nil {
		shardSnap.SplitKeys = si.preSplit.Keys
	}
	for fid, fm := range si.files {
		if fm.level == 0 {
			shardSnap.L0Creates = append(shardSnap.L0Creates, &enginepb.L0Create{
				ID:       fid,
				Smallest: fm.smallest,
				Biggest:  fm.biggest,
			})
		} else {
			shardSnap.TableCreates = append(shardSnap.TableCreates, &enginepb.TableCreate{
				ID:       fid,
				Level:    fm.level,
				CF:       fm.cf,
				Smallest: fm.smallest,
				Biggest:  fm.biggest,
			})
		}
	}
	cs.Snapshot = shardSnap
	if si.parent != nil {
		cs.Parent = si.parent.ToChangeSet()
	}
	return cs
}

func (si *ShardMeta) Marshal() []byte {
	cs := si.ToChangeSet()
	data, _ := cs.Marshal()
	return data
}

func (si *ShardMeta) IsDuplicatedChangeSet(change *enginepb.ChangeSet) bool {
	if change.Sequence > 0 && si.Seq >= change.Sequence {
		log.S().Infof("%d:%d skip duplicated change %s, meta seq:%d",
			si.ID, si.Ver, change, si.Seq)
		return true
	}
	if preSplit := change.PreSplit; preSplit != nil {
		return si.preSplit != nil
	}
	if flush := change.Flush; flush != nil {
		if si.parent != nil {
			return false
		}
		dup := si.commitTS >= flush.CommitTS
		if dup {
			// The new leader triggers flush, which may result in duplicated flush.
			log.S().Infof("%d:%d skip duplicated flush commitTS:%d, meta commitTS:%d",
				si.ID, si.Ver, flush.CommitTS, si.commitTS)
		}
		return dup
	}
	if splitFiles := change.SplitFiles; splitFiles != nil {
		return si.SplitStage == change.Stage
	}
	if comp := change.Compaction; comp != nil {
		if isMoveDown(comp) {
			level, ok := si.FileLevel(comp.TopDeletes[0])
			if ok && level == int(change.Compaction.Level) {
				return false
			}
			log.S().Infof("%d:%d skip duplicated moveDown compaction level:%d",
				si.ID, si.Ver, comp.Level)
			return true
		}
		for _, del := range comp.TopDeletes {
			_, ok := si.FileLevel(del)
			if !ok {
				log.S().Infof("%d:%d skip conflicted compaction file %d already deleted",
					si.ID, si.Ver, del)
				change.Compaction.Conflicted = true
				return true
			}
		}
		for _, del := range comp.BottomDeletes {
			_, ok := si.FileLevel(del)
			if !ok {
				log.S().Infof("%d:%d skip conflicted compaction file %d already deleted",
					si.ID, si.Ver, del)
				change.Compaction.Conflicted = true
				return true
			}
		}
		return false
	}
	return false
}

func (si *ShardMeta) AllFiles() []uint64 {
	var fids []uint64
	for fid := range si.files {
		fids = append(fids, fid)
	}
	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	return fids
}

func (si *ShardMeta) PreSplitKeys() [][]byte {
	return si.preSplit.Keys
}

// LevelCF is the struct that contains shard id and level id,
type LevelCF struct {
	Level uint16
	CF    uint16
}

var GlobalShardEndKey = []byte{255, 255, 255, 255, 255, 255, 255, 255}

func readMetas(reader MetaReader) (map[uint64]*ShardMeta, error) {
	shardMetas := map[uint64]*ShardMeta{}
	err := reader.IterateMeta(func(meta *enginepb.ChangeSet) error {
		shardMetas[meta.ShardID] = NewShardMeta(meta)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return shardMetas, nil
}

func isMoveDown(compaction *enginepb.Compaction) bool {
	return len(compaction.TopDeletes) == len(compaction.TableCreates) &&
		compaction.TopDeletes[0] == compaction.TableCreates[0].ID
}

type fileMeta struct {
	cf       int32
	level    uint32
	smallest []byte
	biggest  []byte
}

func newChangeSet(shard *Shard) *enginepb.ChangeSet {
	return &enginepb.ChangeSet{
		ShardID:  shard.ID,
		ShardVer: shard.Ver,
		Stage:    shard.GetSplitStage(),
	}
}

type MetaReader interface {
	IterateMeta(fn func(meta *enginepb.ChangeSet) error) error
}
