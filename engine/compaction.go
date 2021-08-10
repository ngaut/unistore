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
	"github.com/ngaut/unistore/engine/compaction"
	"github.com/ngaut/unistore/engine/epoch"
	"github.com/ngaut/unistore/engine/table"
	"github.com/ngaut/unistore/engine/table/sstable"
	"github.com/ngaut/unistore/enginepb"
	"github.com/ngaut/unistore/s3util"
	"github.com/ngaut/unistore/scheduler"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type CompactDef struct {
	CF    int
	Level int

	Top []table.Table
	Bot []table.Table

	SafeTS     uint64
	HasOverlap bool
	Opt        sstable.TableBuilderOptions
	Dir        string
	Limiter    *rate.Limiter
	InMemory   bool
	S3         s3util.Options

	thisRange keyRange
	nextRange keyRange

	topSize     int64
	topLeftIdx  int
	topRightIdx int
	botSize     int64
	botLeftIdx  int
	botRightIdx int
}

func (cd *CompactDef) String() string {
	return fmt.Sprintf("%d top:[%d:%d](%d), bot:[%d:%d](%d), write_amp:%.2f",
		cd.Level, cd.topLeftIdx, cd.topRightIdx, cd.topSize,
		cd.botLeftIdx, cd.botRightIdx, cd.botSize, float64(cd.topSize+cd.botSize)/float64(cd.topSize))
}

func (cd *CompactDef) smallest() []byte {
	if len(cd.Bot) > 0 && bytes.Compare(cd.nextRange.left, cd.thisRange.left) < 0 {
		return cd.nextRange.left
	}
	return cd.thisRange.left
}

func (cd *CompactDef) biggest() []byte {
	if len(cd.Bot) > 0 && bytes.Compare(cd.nextRange.right, cd.thisRange.right) > 0 {
		return cd.nextRange.right
	}
	return cd.thisRange.right
}

func (cd *CompactDef) fillTables(thisLevel, nextLevel *levelHandler) bool {
	if len(thisLevel.tables) == 0 {
		return false
	}
	this := make([]table.Table, len(thisLevel.tables))
	copy(this, thisLevel.tables)
	next := make([]table.Table, len(nextLevel.tables))
	copy(next, nextLevel.tables)

	// First pick one table has max topSize/bottomSize ratio.
	var candidateRatio float64
	for i, t := range this {
		left, right := getTablesInRange(next, t.Smallest(), t.Biggest())
		botSize := sumTableSize(next[left:right])
		ratio := calcRatio(t.Size(), botSize)
		if ratio > candidateRatio {
			candidateRatio = ratio
			cd.topLeftIdx = i
			cd.topRightIdx = i + 1
			cd.Top = this[cd.topLeftIdx:cd.topRightIdx:cd.topRightIdx]
			cd.topSize = t.Size()
			cd.botLeftIdx = left
			cd.botRightIdx = right
			cd.botSize = botSize
		}
	}
	if len(cd.Top) == 0 {
		return false
	}
	bots := next[cd.botLeftIdx:cd.botRightIdx:cd.botRightIdx]
	// Expand to left to include more tops as long as the ratio doesn't decrease and the total size
	// do not exceeds maxCompactionExpandSize.
	for i := cd.topLeftIdx - 1; i >= 0; i-- {
		t := this[i]
		left, right := getTablesInRange(next, t.Smallest(), t.Biggest())
		if right < cd.botLeftIdx {
			// A bottom table is skipped, we can compact in another run.
			break
		}
		newTopSize := t.Size() + cd.topSize
		newBotSize := sumTableSize(next[left:cd.botLeftIdx]) + cd.botSize
		cd.Top = append([]table.Table{t}, cd.Top...)
		cd.topLeftIdx--
		bots = append(next[left:cd.botLeftIdx:cd.botLeftIdx], bots...)
		cd.botLeftIdx = left
		cd.topSize = newTopSize
		cd.botSize = newBotSize
	}
	// Expand to right to include more tops as long as the ratio doesn't decrease and the total size
	// do not exceeds maxCompactionExpandSize.
	for i := cd.topRightIdx; i < len(this); i++ {
		t := this[i]
		left, right := getTablesInRange(next, t.Smallest(), t.Biggest())
		if left > cd.botRightIdx {
			// A bottom table is skipped, we can compact in another run.
			break
		}
		newTopSize := t.Size() + cd.topSize
		newBotSize := sumTableSize(next[cd.botRightIdx:right]) + cd.botSize
		cd.Top = append(cd.Top, t)
		cd.topRightIdx++
		bots = append(bots, next[cd.botRightIdx:right]...)
		cd.botRightIdx = right
		cd.topSize = newTopSize
		cd.botSize = newBotSize
	}
	cd.thisRange = keyRange{left: cd.Top[0].Smallest(), right: cd.Top[len(cd.Top)-1].Biggest()}
	if len(bots) > 0 {
		cd.nextRange = keyRange{left: bots[0].Smallest(), right: bots[len(bots)-1].Biggest()}
	} else {
		cd.nextRange = cd.thisRange
	}
	cd.Bot = append(cd.Bot, bots...)
	return true
}

func sumTableSize(tables []table.Table) int64 {
	var size int64
	for _, t := range tables {
		size += t.Size()
	}
	return size
}

func calcRatio(topSize, botSize int64) float64 {
	if botSize == 0 {
		return float64(topSize)
	}
	return float64(topSize) / float64(botSize)
}

func (cd *CompactDef) moveDown() bool {
	return cd.Level > 0 && len(cd.Bot) == 0
}

type keyRange struct {
	left  []byte
	right []byte
}

func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x]", r.left, r.right)
}

func getKeyRange(tables []table.Table) keyRange {
	y.Assert(len(tables) > 0)
	smallest := tables[0].Smallest()
	biggest := tables[0].Biggest()
	for i := 1; i < len(tables); i++ {
		if bytes.Compare(tables[i].Smallest(), smallest) < 0 {
			smallest = tables[i].Smallest()
		}
		if bytes.Compare(tables[i].Biggest(), biggest) > 0 {
			biggest = tables[i].Biggest()
		}
	}
	return keyRange{
		left:  smallest,
		right: biggest,
	}
}

func newTableCreateByResult(result *sstable.BuildResult, cf int, level int) *enginepb.TableCreate {
	return &enginepb.TableCreate{
		ID:       result.ID,
		Level:    uint32(level),
		CF:       int32(cf),
		Smallest: result.Smallest,
		Biggest:  result.Biggest,
	}
}

func newL0CreateByResult(result *sstable.BuildResult) *enginepb.L0Create {
	change := &enginepb.L0Create{
		ID:       result.ID,
		Smallest: result.Smallest,
		Biggest:  result.Biggest,
	}
	return change
}

func (en *Engine) UpdateMangedSafeTs(ts uint64) {
	for {
		old := atomic.LoadUint64(&en.mangedSafeTS)
		if old < ts {
			if !atomic.CompareAndSwapUint64(&en.mangedSafeTS, old, ts) {
				continue
			}
		}
		break
	}
}

func (en *Engine) getCFSafeTS(cf int) uint64 {
	if en.opt.CFs[cf].Managed {
		return atomic.LoadUint64(&en.mangedSafeTS)
	}
	return math.MaxUint64
}

func (en *Engine) getCFMaxLevel(cf int) int {
	return en.opt.CFs[cf].MaxLevels
}

func (en *Engine) runCompactionLoop(c *y.Closer) {
	defer c.Done()
	var priorities []CompactionPriority
	s := scheduler.NewScheduler(en.opt.NumCompactors)
	wg := new(sync.WaitGroup)
	for {
		priorities = en.GetCompactionPriorities(priorities)
		for i := 0; i < en.opt.NumCompactors && i < len(priorities); i++ {
			pri := priorities[i]
			pri.Shard.markCompacting(true)
			wg.Add(1)
			s.Schedule(func() {
				err := en.Compact(pri)
				if err != nil {
					log.Error("compact shard failed", zap.Uint64("shard", pri.Shard.ID), zap.Error(err))
				}
				if err != nil || en.metaChangeListener == nil {
					// When meta change listener is not nil, the compaction will be finished later by applyCompaction.
					pri.Shard.markCompacting(false)
				}
				wg.Done()
			})
		}
		select {
		case <-c.HasBeenClosed():
			wg.Wait()
			return
		case <-time.After(time.Millisecond * 100):
		}
	}
}

type CompactionPriority struct {
	CF    int
	Level int
	Score float64
	Shard *Shard
}

func (en *Engine) getCompactionPriority(shard *Shard) CompactionPriority {
	maxPri := CompactionPriority{Shard: shard}
	l0 := shard.loadL0Tables()
	if l0 != nil {
		sizeScore := float64(l0.totalSize()) / float64(en.opt.BaseSize)
		numTblsScore := float64(len(l0.tables)) / 5
		maxPri.Score = sizeScore*0.7 + numTblsScore*0.3
		maxPri.CF = -1
	}
	for i, scf := range shard.cfs {
		for level := 1; level < len(scf.levels); level++ {
			h := scf.getLevelHandler(level)
			score := float64(h.totalSize) / (float64(en.opt.BaseSize) * math.Pow(10, float64(level-1)))
			if score > maxPri.Score {
				maxPri.Score = score
				maxPri.CF = i
				maxPri.Level = level
			}
		}
	}
	return maxPri
}

func (en *Engine) GetCompactionPriorities(buf []CompactionPriority) []CompactionPriority {
	results := buf[:0]
	en.shardMap.Range(func(key, value interface{}) bool {
		shard := value.(*Shard)
		if !shard.IsPassive() && !shard.isCompacting() {
			pri := en.getCompactionPriority(shard)
			if pri.Score > 1 {
				results = append(results, pri)
			}
		}
		return true
	})
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	return results
}

func (en *Engine) Compact(pri CompactionPriority) error {
	guard := en.resourceMgr.Acquire()
	defer guard.Done()
	shard := pri.Shard
	shard.lock.Lock()
	defer shard.lock.Unlock()
	if shard.isSplitting() {
		log.S().Debugf("avoid compaction for splitting shard.")
		return nil
	}
	latest := en.GetShard(shard.ID)
	if latest.Ver != shard.Ver {
		log.S().Infof("avoid compaction for shard version change.")
		return nil
	}
	if shard.IsPassive() {
		log.S().Warn("avoid active shard compaction.")
		return nil
	}
	if pri.CF == -1 {
		log.Info("compact shard multi cf", zap.Uint64("shard", shard.ID), zap.Float64("score", pri.Score))
		req, err := en.buildCompactL0Request(shard)
		if err != nil {
			return err
		}
		resp := en.compClient.Compact(req)
		return en.handleCompactionResponse(shard, resp, guard)
	}
	log.Info("start compaction", zap.Uint64("shard", shard.ID), zap.Int("cf", pri.CF), zap.Int("level", pri.Level), zap.Float64("score", pri.Score))
	scf := shard.cfs[pri.CF]
	thisLevel := scf.getLevelHandler(pri.Level)
	if len(thisLevel.tables) == 0 {
		// The shard must have been truncated.
		log.Info("stop compaction due to shard truncated", zap.Uint64("shard", shard.ID))
		return nil
	}
	nextLevel := scf.getLevelHandler(pri.Level + 1)
	cd := &CompactDef{
		CF:    pri.CF,
		Level: pri.Level,
	}
	ok := cd.fillTables(thisLevel, nextLevel)
	y.Assert(ok)
	scf.setHasOverlapping(cd)
	log.Info("running compaction", zap.Stringer("def", cd))
	req, err := en.buildCompactLnRequest(cd)
	if err != nil {
		return err
	}
	if req.Level > 0 && len(req.Bottoms) == 0 && req.CF == compaction.WriteCF {
		// Move down. only write CF benefits from this optimization.
		respComp := &enginepb.Compaction{Cf: int32(req.CF), Level: uint32(req.Level), TopDeletes: req.Tops}
		for _, topTbl := range cd.Top {
			tableCreate := &enginepb.TableCreate{
				ID:       topTbl.ID(),
				Level:    uint32(req.Level + 1),
				CF:       int32(req.CF),
				Smallest: topTbl.Smallest(),
				Biggest:  topTbl.Biggest(),
			}
			respComp.TableCreates = append(respComp.TableCreates, tableCreate)
		}
		return en.handleCompactionResponse(shard, &compaction.Response{Compaction: respComp}, guard)
	}
	return en.handleCompactionResponse(shard, en.compClient.Compact(req), guard)
}

func (en *Engine) newCompactionRequest(cf, level int) *compaction.Request {
	req := &compaction.Request{
		CF:           cf,
		Level:        level,
		InstanceID:   en.opt.InstanceID,
		S3:           en.opt.S3Options,
		SafeTS:       atomic.LoadUint64(&en.mangedSafeTS),
		BlockSize:    en.opt.TableBuilderOptions.BlockSize,
		MaxTableSize: en.opt.TableBuilderOptions.MaxTableSize,
		BloomFPR:     en.opt.TableBuilderOptions.LogicalBloomFPR,
	}
	if level == 0 {
		req.Overlap = true
	}
	return req
}

func (en *Engine) buildCompactL0Request(shard *Shard) (*compaction.Request, error) {
	l0s := shard.loadL0Tables()
	req := en.newCompactionRequest(-1, 0)
	var totalSize int64
	for _, l0 := range l0s.tables {
		req.Tops = append(req.Tops, l0.ID())
		totalSize += l0.Size()
	}
	req.MultiCFBottoms = make([][]uint64, en.numCFs)
	for cf := 0; cf < en.numCFs; cf++ {
		levelHandler := shard.cfs[cf].getLevelHandler(1)
		bottoms := make([]uint64, len(levelHandler.tables))
		for i, tbl := range levelHandler.tables {
			bottoms[i] = tbl.ID()
			totalSize += tbl.Size()
		}
		req.MultiCFBottoms[cf] = bottoms
	}
	err := en.setAllocIDForRequest(req, totalSize)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func (en *Engine) buildCompactLnRequest(cd *CompactDef) (*compaction.Request, error) {
	req := en.newCompactionRequest(cd.CF, cd.Level)
	req.Overlap = cd.HasOverlap
	for _, tbl := range cd.Top {
		req.Tops = append(req.Tops, tbl.ID())
	}
	for _, tbl := range cd.Bot {
		req.Bottoms = append(req.Bottoms, tbl.ID())
	}
	err := en.setAllocIDForRequest(req, cd.topSize+cd.botSize)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func (en *Engine) setAllocIDForRequest(req *compaction.Request, totalSize int64) error {
	idCount := totalSize/req.MaxTableSize + 100 // Add 100 here just in case we run out of ID.
	var err error
	req.LastID, err = en.idAlloc.AllocID(int(idCount))
	if err != nil {
		return err
	}
	req.FirstID = req.LastID - uint64(idCount) + 1
	return nil
}

func (en *Engine) handleCompactionResponse(shard *Shard, resp *compaction.Response, guard *epoch.Guard) error {
	if resp.Error != "" {
		err := errors.New(resp.Error)
		log.Error("failed compact L0", zap.Error(err))
		return err
	}
	log.S().Infof("compact shard %d:%d level %d cf %d done",
		shard.ID, shard.Ver, resp.Compaction.Level, resp.Compaction.Cf)
	cs := newChangeSet(shard)
	cs.Compaction = resp.Compaction
	if en.metaChangeListener != nil {
		en.metaChangeListener.OnChange(cs)
		return nil
	}
	return en.applyCompaction(shard, cs, guard)
}

func assertTablesOrder(level int, tables []table.Table, cd *CompactDef) {
	if level == 0 {
		return
	}

	for i := 0; i < len(tables)-1; i++ {
		if bytes.Compare(tables[i].Smallest(), tables[i].Biggest()) > 0 ||
			bytes.Compare(tables[i].Smallest(), tables[i+1].Smallest()) >= 0 ||
			bytes.Compare(tables[i].Biggest(), tables[i+1].Biggest()) >= 0 {

			var sb strings.Builder
			if cd != nil {
				fmt.Fprintf(&sb, "%s\n", cd)
			}
			fmt.Fprintf(&sb, "the order of level %d tables is invalid:\n", level)
			for idx, tbl := range tables {
				tag := "  "
				if idx == i {
					tag = "->"
				}
				fmt.Fprintf(&sb, "%s id:%d smallest:%v biggest:%v\n", tag, tbl.ID(), tbl.Smallest(), tbl.Biggest())
			}
			panic(sb.String())
		}
	}
}

func (en *Engine) replaceTables(old *levelHandler, newTables []table.Table, cd *CompactDef, guard *epoch.Guard) *levelHandler {
	newHandler := newLevelHandler(en.opt.NumLevelZeroTablesStall, old.level)
	newHandler.totalSize = old.totalSize
	// Increase totalSize first.
	for _, tbl := range newTables {
		newHandler.totalSize += tbl.Size()
	}
	left, right := getTablesInRange(old.tables, cd.nextRange.left, cd.nextRange.right)
	toDelete := make([]epoch.Resource, 0, right-left)
	// Update totalSize and reference counts.
	for i := left; i < right; i++ {
		tbl := old.tables[i]
		if containsTable(cd.Bot, tbl) {
			newHandler.totalSize -= tbl.Size()
			toDelete = append(toDelete, &deletion{res: tbl, delete: func() {
				if en.s3c != nil {
					en.s3c.SetExpired(tbl.ID())
				}
			}})
		}
	}
	tables := make([]table.Table, 0, left+len(newTables)+(len(old.tables)-right))
	tables = append(tables, old.tables[:left]...)
	tables = append(tables, newTables...)
	tables = append(tables, old.tables[right:]...)
	sortTables(tables)
	assertTablesOrder(old.level, tables, cd)
	newHandler.tables = tables
	guard.Delete(toDelete)
	return newHandler
}

func containsTable(tables []table.Table, tbl table.Table) bool {
	for _, t := range tables {
		if tbl == t {
			return true
		}
	}
	return false
}

func (en *Engine) deleteTables(old *levelHandler, toDel []table.Table) *levelHandler {
	newHandler := newLevelHandler(en.opt.NumLevelZeroTablesStall, old.level)
	newHandler.totalSize = old.totalSize
	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.ID()] = struct{}{}
	}

	// Make a copy as iterators might be keeping a slice of tables.
	var newTables []table.Table
	for _, t := range old.tables {
		_, found := toDelMap[t.ID()]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		newHandler.totalSize -= t.Size()
	}
	newHandler.tables = newTables

	assertTablesOrder(newHandler.level, newTables, nil)
	return newHandler
}

func (en *Engine) ScheduleResources(f func()) {
	en.resourceScheduler.Schedule(f)
}

func (en *Engine) PrepareResources(changeSet *enginepb.ChangeSet) error {
	if en.s3c == nil {
		return nil
	}
	bt := scheduler.NewBatchTasks()
	if changeSet.Flush != nil {
		flush := changeSet.Flush
		newL0 := flush.L0Create
		if newL0 != nil {
			bt.AppendTask(func() error {
				return en.loadFileFromS3(flush.L0Create.ID)
			})
		}
	}
	if changeSet.Compaction != nil && !isMoveDown(changeSet.Compaction) {
		comp := changeSet.Compaction
		for i := range comp.TableCreates {
			tbl := comp.TableCreates[i]
			bt.AppendTask(func() error {
				return en.loadFileFromS3(tbl.ID)
			})
		}
	}
	if changeSet.SplitFiles != nil {
		splitFiles := changeSet.SplitFiles
		for i := range splitFiles.L0Creates {
			l0 := splitFiles.L0Creates[i]
			bt.AppendTask(func() error {
				return en.loadFileFromS3(l0.ID)
			})
		}
		for i := range splitFiles.TableCreates {
			tbl := splitFiles.TableCreates[i]
			bt.AppendTask(func() error {
				return en.loadFileFromS3(tbl.ID)
			})
		}
	}
	if changeSet.Snapshot != nil {
		snap := changeSet.Snapshot
		for i := range snap.L0Creates {
			l0 := snap.L0Creates[i]
			bt.AppendTask(func() error {
				return en.loadFileFromS3(l0.ID)
			})
		}
		for i := range snap.TableCreates {
			tbl := snap.TableCreates[i]
			bt.AppendTask(func() error {
				return en.loadFileFromS3(tbl.ID)
			})
		}
	}
	return en.s3c.BatchSchedule(bt)
}

// ApplyChangeSet applies long running shard file change.
// Includes:
//   - mem-table flush.
//   - compaction.
//   - split-files.
func (en *Engine) ApplyChangeSet(changeSet *enginepb.ChangeSet) error {
	guard := en.resourceMgr.Acquire()
	defer guard.Done()
	shard := en.GetShard(changeSet.ShardID)
	if shard.Ver != changeSet.ShardVer {
		return ErrShardNotMatch
	}
	if seq := atomic.LoadUint64(&shard.sequence); seq >= changeSet.Sequence {
		log.S().Infof("%d:%d skip duplicated change %s", shard.ID, shard.Ver, changeSet)
		return nil
	} else {
		atomic.CompareAndSwapUint64(&shard.sequence, seq, changeSet.Sequence)
	}
	defer func() {
		log.S().Debugf("%d:%d all files after change %v", shard.ID, shard.Ver, shard.GetAllFiles())
	}()
	defer shard.refreshEstimatedSize()
	if changeSet.Flush != nil {
		return en.applyFlush(shard, changeSet)
	}
	if changeSet.Compaction != nil {
		return en.applyCompaction(shard, changeSet, guard)
	}
	if changeSet.SplitFiles != nil {
		return en.applySplitFiles(shard, changeSet, guard)
	}
	return nil
}

func (en *Engine) applyFlush(shard *Shard, changeSet *enginepb.ChangeSet) error {
	flush := changeSet.Flush
	newL0 := flush.L0Create
	if newL0 != nil {
		filename := sstable.NewFilename(newL0.ID, en.opt.Dir)
		localFile, err := sstable.NewLocalFile(filename, true)
		if err != nil {
			return err
		}
		tbl, err := sstable.OpenL0Table(localFile)
		if err != nil {
			return err
		}
		atomicAddL0(shard, tbl)
		atomicRemoveMemTable(shard)
	}
	shard.setCommitTS(flush.CommitTS)
	shard.setSplitStage(changeSet.Stage)
	shard.setInitialFlushed()
	return nil
}

func (en *Engine) applyCompaction(shard *Shard, changeSet *enginepb.ChangeSet, guard *epoch.Guard) error {
	defer shard.markCompacting(false)
	comp := changeSet.Compaction
	if comp.Conflicted {
		if isMoveDown(comp) {
			return nil
		}
		var resources []epoch.Resource
		for i := range comp.TableCreates {
			tbl := comp.TableCreates[i]
			resources = append(resources, &deletion{
				delete: func() {
					os.Remove(sstable.NewFilename(tbl.ID, en.opt.Dir))
					if en.s3c != nil {
						en.s3c.SetExpired(tbl.ID)
					}
				},
			})
		}
		guard.Delete(resources)
		return nil
	}
	del := &deletions{resources: map[uint64]epoch.Resource{}}
	if comp.Level == 0 {
		l0Tbls := shard.loadL0Tables()
		if l0Tbls != nil {
			for _, tbl := range l0Tbls.tables {
				if containsUint64(comp.TopDeletes, tbl.ID()) {
					res := tbl
					del.add(res.ID(), &deletion{res: res, delete: func() {
						if en.s3c != nil {
							en.s3c.SetExpired(res.ID())
						}
					}})
				}
			}
		}
		for cf := 0; cf < en.numCFs; cf++ {
			err := en.compactionUpdateLevelHandler(shard, cf, 1, comp.TableCreates, comp.BottomDeletes, del)
			if err != nil {
				return err
			}
		}
		atomicRemoveL0(shard, len(comp.TopDeletes))
	} else {
		err := en.compactionUpdateLevelHandler(shard, int(comp.Cf), int(comp.Level+1), comp.TableCreates, comp.BottomDeletes, del)
		if err != nil {
			return err
		}
		err = en.compactionUpdateLevelHandler(shard, int(comp.Cf), int(comp.Level), nil, comp.TopDeletes, del)
		if err != nil {
			return err
		}
		// For move down operation, the TableCreates may contains TopDeletes, we don't want to delete them.
		for _, create := range comp.TableCreates {
			del.remove(create.ID)
		}
	}
	guard.Delete(del.collect())
	return nil
}

type deletion struct {
	res    epoch.Resource
	delete func()
}

func (d *deletion) Delete() error {
	if d.delete != nil {
		d.delete()
	}
	if d.res != nil {
		return d.res.Delete()
	}
	return nil
}

func (en *Engine) compactionUpdateLevelHandler(shard *Shard, cf, level int,
	creates []*enginepb.TableCreate, delIDs []uint64, del *deletions) error {
	oldLevel := shard.cfs[cf].getLevelHandler(level)
	newLevel := newLevelHandler(en.opt.NumLevelZeroTablesStall, level)
	for _, tbl := range creates {
		if int(tbl.CF) != cf {
			continue
		}
		filename := sstable.NewFilename(tbl.ID, en.opt.Dir)
		reader, err := newTableFile(filename, en)
		if err != nil {
			return err
		}
		tbl, err := sstable.OpenTable(reader, en.blkCache)
		if err != nil {
			return err
		}
		newLevel.tables = append(newLevel.tables, tbl)
		newLevel.totalSize += tbl.Size()
	}
	for _, oldTbl := range oldLevel.tables {
		if containsUint64(delIDs, oldTbl.ID()) {
			res := oldTbl
			del.add(res.ID(), &deletion{res: res, delete: func() {
				if en.s3c != nil {
					en.s3c.SetExpired(res.ID())
				}
			}})
		} else {
			newLevel.tables = append(newLevel.tables, oldTbl)
			newLevel.totalSize += oldTbl.Size()
		}
	}
	sortTables(newLevel.tables)
	assertTablesOrder(level, newLevel.tables, nil)
	shard.cfs[cf].casLevelHandler(level, oldLevel, newLevel)
	return nil
}

func (en *Engine) applySplitFiles(shard *Shard, changeSet *enginepb.ChangeSet, guard *epoch.Guard) error {
	if shard.GetSplitStage() != enginepb.SplitStage_PRE_SPLIT_FLUSH_DONE {
		log.S().Errorf("wrong split stage %s", shard.GetSplitStage())
		return ErrSplitFilesWrongStage
	}
	splitFiles := changeSet.SplitFiles
	oldL0s := shard.loadL0Tables()
	newL0Tbls := &l0Tables{make([]*sstable.L0Table, 0, len(oldL0s.tables)*2)}
	del := &deletions{resources: map[uint64]epoch.Resource{}}
	for _, l0 := range splitFiles.L0Creates {
		filename := sstable.NewFilename(l0.ID, en.opt.Dir)
		localFile, err := sstable.NewLocalFile(filename, true)
		tbl, err := sstable.OpenL0Table(localFile)
		if err != nil {
			return err
		}
		newL0Tbls.tables = append(newL0Tbls.tables, tbl)
	}
	for _, oldL0 := range oldL0s.tables {
		if containsUint64(splitFiles.TableDeletes, oldL0.ID()) {
			res := oldL0
			del.add(res.ID(), &deletion{res: res, delete: func() {
				if en.s3c != nil {
					en.s3c.SetExpired(res.ID())
				}
			}})
		} else {
			newL0Tbls.tables = append(newL0Tbls.tables, oldL0)
		}
	}
	sort.Slice(newL0Tbls.tables, func(i, j int) bool {
		return newL0Tbls.tables[i].CommitTS() > newL0Tbls.tables[j].CommitTS()
	})
	y.Assert(atomic.CompareAndSwapPointer(shard.l0s, unsafe.Pointer(oldL0s), unsafe.Pointer(newL0Tbls)))
	newHandlers := make([][]*levelHandler, en.numCFs)
	for cf := 0; cf < en.numCFs; cf++ {
		newHandlers[cf] = make([]*levelHandler, en.getCFMaxLevel(cf))
	}
	for _, tbl := range splitFiles.TableCreates {
		newHandler := newHandlers[tbl.CF][tbl.Level-1]
		if newHandler == nil {
			newHandler = newLevelHandler(en.opt.NumLevelZeroTablesStall, int(tbl.Level))
			newHandlers[tbl.CF][tbl.Level-1] = newHandler
		}
		filename := sstable.NewFilename(tbl.ID, en.opt.Dir)
		reader, err := newTableFile(filename, en)
		if err != nil {
			return err
		}
		tbl, err := sstable.OpenTable(reader, en.blkCache)
		if err != nil {
			return err
		}
		newHandler.totalSize += tbl.Size()
		newHandler.tables = append(newHandler.tables, tbl)
	}
	for cf := 0; cf < en.numCFs; cf++ {
		for level := 1; level <= en.getCFMaxLevel(cf); level++ {
			newHandler := newHandlers[cf][level-1]
			if newHandler == nil {
				continue
			}
			oldHandler := shard.cfs[cf].getLevelHandler(level)
			for _, oldTbl := range oldHandler.tables {
				if containsUint64(splitFiles.TableDeletes, oldTbl.ID()) {
					res := oldTbl
					del.add(res.ID(), &deletion{res: res, delete: func() {
						if en.s3c != nil {
							en.s3c.SetExpired(res.ID())
						}
					}})
				} else {
					newHandler.totalSize += oldTbl.Size()
					newHandler.tables = append(newHandler.tables, oldTbl)
				}
			}
			sort.Slice(newHandler.tables, func(i, j int) bool {
				return bytes.Compare(newHandler.tables[i].Smallest(), newHandler.tables[j].Smallest()) < 0
			})
			y.Assert(shard.cfs[cf].casLevelHandler(level, oldHandler, newHandler))
		}
	}
	shard.setSplitStage(changeSet.Stage)
	guard.Delete(del.collect())
	return nil
}

func containsUint64(slice []uint64, v uint64) bool {
	for _, sv := range slice {
		if sv == v {
			return true
		}
	}
	return false
}

type DiscardStats struct {
	numSkips     int64
	skippedBytes int64
}

func (ds *DiscardStats) collect(vs y.ValueStruct) {
	ds.skippedBytes += int64(len(vs.Value) + len(vs.UserMeta))
	ds.numSkips++
}

func (ds *DiscardStats) String() string {
	return fmt.Sprintf("numSkips:%d, skippedBytes:%d", ds.numSkips, ds.skippedBytes)
}

func putSSTBuildResultToS3(s3c *s3util.S3Client, result *sstable.BuildResult) (err error) {
	return s3c.Put(s3c.BlockKey(result.ID), result.FileData)
}

func sortTables(tables []table.Table) {
	sort.Slice(tables, func(i, j int) bool {
		return bytes.Compare(tables[i].Smallest(), tables[j].Smallest()) < 0
	})
}
