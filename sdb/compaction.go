package sdb

import (
	"bytes"
	"fmt"
	"github.com/ngaut/unistore/s3util"
	"github.com/ngaut/unistore/sdb/epoch"
	"github.com/ngaut/unistore/sdb/table"
	"github.com/ngaut/unistore/sdb/table/sstable"
	"github.com/ngaut/unistore/sdbpb"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"io"
	"io/ioutil"
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

	SkippedTbls []table.Table
	SafeTS      uint64
	Filter      CompactionFilter
	HasOverlap  bool
	Opt         sstable.TableBuilderOptions
	Dir         string
	AllocIDFunc func() uint64
	Limiter     *rate.Limiter
	InMemory    bool
	S3          s3util.Options

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
	return fmt.Sprintf("%d top:[%d:%d](%d), bot:[%d:%d](%d), skip:%d, write_amp:%.2f",
		cd.Level, cd.topLeftIdx, cd.topRightIdx, cd.topSize,
		cd.botLeftIdx, cd.botRightIdx, cd.botSize, len(cd.SkippedTbls), float64(cd.topSize+cd.botSize)/float64(cd.topSize))
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
	cd.fillBottomTables(bots)
	for _, t := range cd.SkippedTbls {
		cd.botSize -= t.Size()
	}
	return true
}

const minSkippedTableSize = 1024 * 1024

func (cd *CompactDef) fillBottomTables(overlappingTables []table.Table) {
	for _, t := range overlappingTables {
		// If none of the Top tables contains the range in an overlapping bottom table,
		// we can skip it during compaction to reduce write amplification.
		var added bool
		for _, topTbl := range cd.Top {
			if topTbl.HasOverlap(t.Smallest(), t.Biggest(), true) {
				cd.Bot = append(cd.Bot, t)
				added = true
				break
			}
		}
		if !added {
			if t.Size() >= minSkippedTableSize {
				// We need to limit the minimum size of the table to be skipped,
				// otherwise the number of tables in a level will keep growing
				// until we meet too many open files error.
				cd.SkippedTbls = append(cd.SkippedTbls, t)
			} else {
				cd.Bot = append(cd.Bot, t)
			}
		}
	}
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
	return cd.Level > 0 && len(cd.Bot) == 0 && len(cd.SkippedTbls) == 0
}

func (cd *CompactDef) buildIterator() table.Iterator {
	// Create iterators across all the tables involved first.
	var iters []table.Iterator
	if cd.Level == 0 {
		iters = appendIteratorsReversed(iters, cd.Top, false)
	} else {
		iters = []table.Iterator{table.NewConcatIterator(cd.Top, false)}
	}

	// Next level has level>=1 and we can use ConcatIterator as key ranges do not overlap.
	iters = append(iters, table.NewConcatIterator(cd.Bot, false))
	it := table.NewMergeIterator(iters, false)

	it.Rewind()
	return it
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

type compactor interface {
	compact(cd *CompactDef, stats *y.CompactionStats, discardStats *DiscardStats) ([]*sstable.BuildResult, error)
}

type localCompactor struct {
	s3c *s3util.S3Client
}

func (c *localCompactor) compact(cd *CompactDef, stats *y.CompactionStats, discardStats *DiscardStats) ([]*sstable.BuildResult, error) {
	return CompactTables(cd, stats, discardStats, c.s3c)
}

func newTableCreate(tbl table.Table, cf int, level int) *sdbpb.TableCreate {
	return &sdbpb.TableCreate{
		ID:       tbl.ID(),
		Level:    uint32(level),
		CF:       int32(cf),
		Smallest: tbl.Smallest(),
		Biggest:  tbl.Biggest(),
	}
}

func newTableCreateByResult(result *sstable.BuildResult, cf int, level int) *sdbpb.TableCreate {
	id, ok := sstable.ParseFileID(result.FileName)
	y.Assert(ok)
	return &sdbpb.TableCreate{
		ID:       id,
		Level:    uint32(level),
		CF:       int32(cf),
		Smallest: result.Smallest,
		Biggest:  result.Biggest,
	}
}

func newL0CreateByResult(result *sstable.BuildResult, props *sdbpb.Properties) *sdbpb.L0Create {
	id, ok := sstable.ParseFileID(result.FileName)
	y.Assert(ok)
	change := &sdbpb.L0Create{
		ID:         id,
		Start:      result.Smallest,
		End:        result.Biggest,
		Properties: props,
	}
	return change
}

func (sdb *DB) UpdateMangedSafeTs(ts uint64) {
	for {
		old := atomic.LoadUint64(&sdb.mangedSafeTS)
		if old < ts {
			if !atomic.CompareAndSwapUint64(&sdb.mangedSafeTS, old, ts) {
				continue
			}
		}
		break
	}
}

func (sdb *DB) getCFSafeTS(cf int) uint64 {
	if sdb.opt.CFs[cf].Managed {
		return atomic.LoadUint64(&sdb.mangedSafeTS)
	}
	return atomic.LoadUint64(&sdb.safeTsTracker.safeTs)
}

type compactL0Helper struct {
	cf         int
	db         *DB
	builder    *sstable.Builder
	shard      *Shard
	l0Tbls     *l0Tables
	lastKey    []byte
	skipKey    []byte
	safeTS     uint64
	filter     CompactionFilter
	iter       table.Iterator
	oldHandler *levelHandler
}

func newCompactL0Helper(db *DB, shard *Shard, l0Tbls *l0Tables, cf int) *compactL0Helper {
	helper := &compactL0Helper{db: db, shard: shard, cf: cf}
	if db.opt.CompactionFilterFactory != nil {
		helper.filter = db.opt.CompactionFilterFactory(1, nil, globalShardEndKey)
	}
	helper.safeTS = db.getCFSafeTS(cf)
	var iters []table.Iterator
	if l0Tbls != nil {
		for _, tbl := range l0Tbls.tables {
			it := tbl.NewIterator(cf, false)
			if it != nil {
				iters = append(iters, it)
			}
		}
	}
	helper.oldHandler = shard.cfs[cf].getLevelHandler(1)
	if len(helper.oldHandler.tables) > 0 {
		iters = append(iters, table.NewConcatIterator(helper.oldHandler.tables, false))
	}
	helper.iter = table.NewMergeIterator(iters, false)
	helper.iter.Rewind()
	return helper
}

func (h *compactL0Helper) setFID(fid uint64) {
	if h.builder == nil {
		h.builder = sstable.NewTableBuilder(fid, h.db.opt.TableBuilderOptions)
	} else {
		h.builder.Reset(fid)
	}
}

func (h *compactL0Helper) buildOne() (*sstable.BuildResult, error) {
	id := h.db.idAlloc.AllocID()
	filename := sstable.NewFilename(id, h.db.opt.Dir)
	fd, err := y.OpenSyncedFile(filename, false)
	if err != nil {
		return nil, err
	}
	h.setFID(id)
	h.lastKey = h.lastKey[:0]
	h.skipKey = h.skipKey[:0]
	it := h.iter
	rc := h.db.opt.CFs[h.cf].ReadCommitted
	for ; it.Valid(); table.NextAllVersion(it) {
		vs := it.Value()
		key := it.Key()
		// See if we need to skip this key.
		if len(h.skipKey) > 0 {
			if bytes.Equal(key, h.skipKey) {
				continue
			} else {
				h.skipKey = h.skipKey[:0]
			}
		}
		if !bytes.Equal(key, h.lastKey) {
			// We only break on table size.
			if h.builder.EstimateSize() > int(h.db.opt.TableBuilderOptions.MaxTableSize) {
				break
			}
			h.lastKey = append(h.lastKey[:0], key...)
		}

		// Only consider the versions which are below the safeTS, otherwise, we might end up discarding the
		// only valid version for a running transaction.
		if rc || vs.Version <= h.safeTS {
			// key is the latest readable version of this key, so we simply discard all the rest of the versions.
			h.skipKey = append(h.skipKey[:0], key...)
			if !isDeleted(vs.Meta) && h.filter != nil {
				switch h.filter.Filter(h.cf, key, vs.Value, vs.UserMeta) {
				case DecisionMarkTombstone:
					// There may have ole versions for this key, so convert to delete tombstone.
					h.builder.Add(key, &y.ValueStruct{Meta: bitDelete, Version: vs.Version})
					continue
				case DecisionDrop:
					continue
				case DecisionKeep:
				}
			}
		}
		h.builder.Add(key, &vs)
	}
	if h.builder.Empty() {
		return nil, nil
	}
	result, err := h.builder.Finish(fd.Name(), fd)
	if err != nil {
		return nil, err
	}
	fd.Sync()
	fd.Close()
	return result, nil
}

func (sdb *DB) compactL0(shard *Shard, guard *epoch.Guard) error {
	l0Tbls := shard.loadL0Tables()
	comp := &sdbpb.Compaction{}
	var toBeDelete []epoch.Resource
	var shardSizeChange int64
	for cf := 0; cf < sdb.numCFs; cf++ {
		helper := newCompactL0Helper(sdb, shard, l0Tbls, cf)
		defer helper.iter.Close()
		var results []*sstable.BuildResult
		for {
			result, err := helper.buildOne()
			if err != nil {
				return err
			}
			if result == nil {
				break
			}
			if sdb.s3c != nil {
				err = putSSTBuildResultToS3(sdb.s3c, result)
				if err != nil {
					return err
				}
			}
			results = append(results, result)
		}
		for _, result := range results {
			comp.TableCreates = append(comp.TableCreates, newTableCreateByResult(result, cf, 1))
		}
		for _, oldTbl := range helper.oldHandler.tables {
			comp.BottomDeletes = append(comp.BottomDeletes, oldTbl.ID())
			toBeDelete = append(toBeDelete, oldTbl)
		}
	}
	if l0Tbls != nil {
		// A splitting shard does not run compaction.
		for _, tbl := range l0Tbls.tables {
			comp.TopDeletes = append(comp.TopDeletes, tbl.ID())
			toBeDelete = append(toBeDelete, tbl)
			shardSizeChange -= tbl.Size()
		}
	}
	change := newChangeSet(shard)
	change.Compaction = comp
	log.S().Infof("shard %d:%d compact L0 top deletes %v", shard.ID, shard.Ver, comp.TopDeletes)
	if sdb.metaChangeListener != nil {
		sdb.metaChangeListener.OnChange(change)
		return nil
	}
	return sdb.applyCompaction(shard, change, guard)
}

func (sdb *DB) runCompactionLoop(c *y.Closer) {
	defer c.Done()
	var priorities []CompactionPriority
	for {
		priorities = sdb.GetCompactionPriorities(priorities)
		wg := new(sync.WaitGroup)
		for i := 0; i < sdb.opt.NumCompactors && i < len(priorities); i++ {
			pri := priorities[i]
			pri.Shard.markCompacting(true)
			wg.Add(1)
			go func() {
				err := sdb.Compact(pri)
				if err != nil {
					log.Error("compact shard failed", zap.Uint64("shard", pri.Shard.ID), zap.Error(err))
				}
				if err != nil || sdb.metaChangeListener == nil {
					// When meta change listener is not nil, the compaction will be finished later by applyCompaction.
					pri.Shard.markCompacting(false)
				}
				wg.Done()
			}()
		}
		wg.Wait()
		select {
		case <-c.HasBeenClosed():
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

func (sdb *DB) getCompactionPriority(shard *Shard) CompactionPriority {
	maxPri := CompactionPriority{Shard: shard}
	l0 := shard.loadL0Tables()
	if l0 != nil && len(l0.tables) > sdb.opt.NumLevelZeroTables {
		sizeScore := float64(l0.totalSize()) * 10 / float64(sdb.opt.LevelOneSize)
		numTblsScore := float64(len(l0.tables)) / float64(sdb.opt.NumLevelZeroTables)
		maxPri.Score = sizeScore*0.6 + numTblsScore*0.4
		maxPri.CF = -1
		return maxPri
	}
	for i, scf := range shard.cfs {
		for level := 1; level < ShardMaxLevel; level++ {
			h := scf.getLevelHandler(level)
			score := float64(h.totalSize) / (float64(sdb.opt.LevelOneSize) * math.Pow(10, float64(level-1)))
			if score > maxPri.Score {
				maxPri.Score = score
				maxPri.CF = i
				maxPri.Level = level
			}
		}
	}
	return maxPri
}

func (sdb *DB) GetCompactionPriorities(buf []CompactionPriority) []CompactionPriority {
	results := buf[:0]
	sdb.shardMap.Range(func(key, value interface{}) bool {
		shard := value.(*Shard)
		if !shard.IsPassive() && atomic.LoadInt32(&shard.compacting) == 0 {
			pri := sdb.getCompactionPriority(shard)
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

func (sdb *DB) Compact(pri CompactionPriority) error {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	shard := pri.Shard
	shard.lock.Lock()
	defer shard.lock.Unlock()
	if shard.isSplitting() {
		log.S().Debugf("avoid compaction for splitting shard.")
		return nil
	}
	latest := sdb.GetShard(shard.ID)
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
		err := sdb.compactL0(shard, guard)
		log.Info("compact shard multi cf done", zap.Uint64("shard", shard.ID), zap.Error(err))
		return err
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
	if err := sdb.runCompactionDef(shard, pri.CF, cd, guard); err != nil {
		// This compaction couldn't be done successfully.
		log.Error("compact failed", zap.Stringer("def", cd), zap.Error(err))
		return err
	}
	log.Info("compaction done", zap.Int("level", cd.Level))
	return nil
}

func (sdb *DB) runCompactionDef(shard *Shard, cf int, cd *CompactDef, guard *epoch.Guard) error {
	var newTables []table.Table
	comp := &sdbpb.Compaction{Cf: int32(cf), Level: uint32(cd.Level)}
	if cd.moveDown() {
		// skip level 0, since it may has many table overlap with each other
		newTables = cd.Top
		for _, t := range newTables {
			comp.TopDeletes = append(comp.TopDeletes, t.ID())
			comp.TableCreates = append(comp.TableCreates, newTableCreate(t, cf, cd.Level+1))
		}
	} else {
		var err error
		comp.TableCreates, err = sdb.compactBuildTables(cf, cd)
		if err != nil {
			return errors.WithStack(err)
		}
		for _, t := range cd.Top {
			comp.TopDeletes = append(comp.TopDeletes, t.ID())
		}
		for _, t := range cd.Bot {
			comp.BottomDeletes = append(comp.BottomDeletes, t.ID())
		}
	}
	change := newChangeSet(shard)
	change.Compaction = comp
	if sdb.metaChangeListener != nil {
		sdb.metaChangeListener.OnChange(change)
		return nil
	}
	return sdb.applyCompaction(shard, change, guard)
}

func getTblIDs(tables []table.Table) []uint64 {
	var ids []uint64
	for _, tbl := range tables {
		ids = append(ids, tbl.ID())
	}
	return ids
}

func getShardIDs(shards []*Shard) []uint64 {
	var ids []uint64
	for _, s := range shards {
		ids = append(ids, s.ID)
	}
	return ids
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

func (sdb *DB) replaceTables(old *levelHandler, newTables []table.Table, cd *CompactDef, guard *epoch.Guard) *levelHandler {
	newHandler := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, old.level, sdb.metrics)
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
			toDelete = append(toDelete, tbl)
		}
	}
	tables := make([]table.Table, 0, left+len(newTables)+len(cd.SkippedTbls)+(len(old.tables)-right))
	tables = append(tables, old.tables[:left]...)
	tables = append(tables, newTables...)
	tables = append(tables, cd.SkippedTbls...)
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

func (sdb *DB) deleteTables(old *levelHandler, toDel []table.Table) *levelHandler {
	newHandler := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, old.level, sdb.metrics)
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

func (sdb *DB) compactBuildTables(cf int, cd *CompactDef) (newTables []*sdbpb.TableCreate, err error) {
	err = sdb.prepareCompactionDef(cf, cd)
	if err != nil {
		return nil, err
	}
	stats := &y.CompactionStats{}
	discardStats := &DiscardStats{}
	comp := &localCompactor{s3c: sdb.s3c}
	buildResults, err := comp.compact(cd, stats, discardStats)
	if err != nil {
		return nil, err
	}
	newTables = make([]*sdbpb.TableCreate, len(buildResults))
	for i, buildResult := range buildResults {
		newTables[i] = newTableCreateByResult(buildResult, cf, cd.Level+1)
	}
	return newTables, nil
}

func (sdb *DB) prepareCompactionDef(cf int, cd *CompactDef) error {
	// Pick up the currently pending transactions' min readTs, so we can discard versions below this
	// readTs. We should never discard any versions starting from above this timestamp, because that
	// would affect the snapshot view guarantee provided by transactions.
	cd.SafeTS = sdb.getCFSafeTS(cf)
	if sdb.opt.CompactionFilterFactory != nil {
		cd.Filter = sdb.opt.CompactionFilterFactory(cd.Level+1, cd.smallest(), cd.biggest())
	}
	cd.Opt = sdb.opt.TableBuilderOptions
	cd.Dir = sdb.opt.Dir
	cd.AllocIDFunc = sdb.idAlloc.AllocID
	cd.S3 = sdb.opt.S3Options
	for _, t := range cd.Top {
		if sst, ok := t.(*sstable.Table); ok {
			err := sst.PrepareForCompaction()
			if err != nil {
				return err
			}
		}
	}
	for _, t := range cd.Bot {
		if sst, ok := t.(*sstable.Table); ok {
			err := sst.PrepareForCompaction()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (sdb *DB) openTables(buildResults []*sstable.BuildResult) (newTables []table.Table, err error) {
	for _, result := range buildResults {
		var tbl table.Table
		filename := result.FileName
		reader, err := newTableFile(filename, sdb)
		if err != nil {
			return nil, err
		}
		tbl, err = sstable.OpenTable(reader, sdb.blkCache)
		if err != nil {
			return nil, err
		}
		newTables = append(newTables, tbl)
	}
	// Ensure created files' directory entries are visible.  We don't mind the extra latency
	// from not doing this ASAP after all file creation has finished because this is a
	// background operation.
	err = syncDir(sdb.opt.Dir)
	if err != nil {
		log.Error("compact sync dir error", zap.Error(err))
		return
	}
	sortTables(newTables)
	return
}

// ApplyChangeSet applies long running shard file change.
// Includes:
//   - mem-table flush.
//   - compaction.
//   - split-files.
func (sdb *DB) ApplyChangeSet(changeSet *sdbpb.ChangeSet) error {
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	shard := sdb.GetShard(changeSet.ShardID)
	if shard.Ver != changeSet.ShardVer {
		return errShardNotMatch
	}
	if changeSet.Flush != nil {
		return sdb.applyFlush(shard, changeSet)
	}
	if changeSet.Compaction != nil {
		return sdb.applyCompaction(shard, changeSet, guard)
	}
	if changeSet.SplitFiles != nil {
		return sdb.applySplitFiles(shard, changeSet, guard)
	}
	return nil
}

func (sdb *DB) applyFlush(shard *Shard, changeSet *sdbpb.ChangeSet) error {
	flush := changeSet.Flush
	bt := s3util.NewBatchTasks()
	newL0 := flush.L0Create
	if newL0 != nil {
		bt.AppendTask(func() error {
			return sdb.loadFileFromS3(flush.L0Create.ID)
		})
	}
	if sdb.s3c != nil {
		if err := sdb.s3c.BatchSchedule(bt); err != nil {
			return err
		}
	}
	if err := sdb.manifest.writeChangeSet(changeSet); err != nil {
		if err == errDupChange {
			return nil
		}
		return err
	}
	if newL0 != nil {
		filename := sstable.NewFilename(newL0.ID, sdb.opt.Dir)
		tbl, err := sstable.OpenL0Table(filename, newL0.ID)
		if err != nil {
			return err
		}
		shard.addEstimatedSize(tbl.Size())
		atomicAddL0(shard, tbl)
		atomicRemoveMemTable(shard.memTbls, 1)
	}
	shard.setSplitState(changeSet.State)
	shard.setInitialFlushed()
	return nil
}

func (sdb *DB) applyCompaction(shard *Shard, changeSet *sdbpb.ChangeSet, guard *epoch.Guard) error {
	defer shard.markCompacting(false)
	comp := changeSet.Compaction
	if sdb.s3c != nil {
		bt := s3util.NewBatchTasks()
		for i := range comp.TableCreates {
			tbl := comp.TableCreates[i]
			bt.AppendTask(func() error {
				return sdb.loadFileFromS3(tbl.ID)
			})
		}
		if err := sdb.s3c.BatchSchedule(bt); err != nil {
			return err
		}
	}
	if err := sdb.manifest.writeChangeSet(changeSet); err != nil {
		if err == errDupChange {
			return nil
		}
		return err
	}
	del := &deletions{resources: map[uint64]epoch.Resource{}}
	if comp.Level == 0 {
		l0Tbls := shard.loadL0Tables()
		if l0Tbls != nil {
			for _, tbl := range l0Tbls.tables {
				if containsUint64(comp.TopDeletes, tbl.ID()) {
					del.add(tbl.ID(), tbl)
				}
			}
		}
		for cf := 0; cf < sdb.numCFs; cf++ {
			err := sdb.compactionUpdateLevelHandler(shard, cf, 1, comp.TableCreates, comp.BottomDeletes, del)
			if err != nil {
				return err
			}
		}
		shard.addEstimatedSize(-atomicRemoveL0(shard, len(comp.TopDeletes)))
	} else {
		err := sdb.compactionUpdateLevelHandler(shard, int(comp.Cf), int(comp.Level), nil, comp.TopDeletes, del)
		if err != nil {
			return err
		}
		// For move down operation, the TableCreates may contains TopDeletes, we don't want to delete them.
		for _, create := range comp.TableCreates {
			del.remove(create.ID)
		}
		err = sdb.compactionUpdateLevelHandler(shard, int(comp.Cf), int(comp.Level+1), comp.TableCreates, comp.BottomDeletes, del)
		if err != nil {
			return err
		}
	}
	guard.Delete(del.collect())
	return nil
}

func (sdb *DB) compactionUpdateLevelHandler(shard *Shard, cf, level int,
	creates []*sdbpb.TableCreate, delIDs []uint64, del *deletions) error {
	oldLevel := shard.cfs[cf].getLevelHandler(level)
	newLevel := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, level, sdb.metrics)
	for _, tbl := range creates {
		if int(tbl.CF) != cf {
			continue
		}
		filename := sstable.NewFilename(tbl.ID, sdb.opt.Dir)
		reader, err := newTableFile(filename, sdb)
		if err != nil {
			return err
		}
		tbl, err := sstable.OpenTable(reader, sdb.blkCache)
		if err != nil {
			return err
		}
		newLevel.tables = append(newLevel.tables, tbl)
		newLevel.totalSize += tbl.Size()
	}
	for _, oldTbl := range oldLevel.tables {
		if containsUint64(delIDs, oldTbl.ID()) {
			del.add(oldTbl.ID(), oldTbl)
		} else {
			newLevel.tables = append(newLevel.tables, oldTbl)
			newLevel.totalSize += oldTbl.Size()
		}
	}
	sortTables(newLevel.tables)
	assertTablesOrder(level, newLevel.tables, nil)
	shard.cfs[cf].casLevelHandler(level, oldLevel, newLevel)
	shard.addEstimatedSize(newLevel.totalSize - oldLevel.totalSize)
	return nil
}

func (sdb *DB) applySplitFiles(shard *Shard, changeSet *sdbpb.ChangeSet, guard *epoch.Guard) error {
	if shard.GetSplitState() != sdbpb.SplitState_PRE_SPLIT_FLUSH_DONE {
		log.S().Errorf("wrong split state %s", shard.GetSplitState())
		return errShardWrongSplittingState
	}
	splitFiles := changeSet.SplitFiles
	bt := s3util.NewBatchTasks()
	for i := range splitFiles.L0Creates {
		l0 := splitFiles.L0Creates[i]
		bt.AppendTask(func() error {
			return sdb.loadFileFromS3(l0.ID)
		})
	}
	if sdb.s3c != nil {
		if err := sdb.s3c.BatchSchedule(bt); err != nil {
			return err
		}
	}
	bt = s3util.NewBatchTasks()
	for i := range splitFiles.TableCreates {
		tbl := splitFiles.TableCreates[i]
		bt.AppendTask(func() error {
			return sdb.loadFileFromS3(tbl.ID)
		})
	}
	if sdb.s3c != nil {
		if err := sdb.s3c.BatchSchedule(bt); err != nil {
			return err
		}
	}
	if err := sdb.manifest.writeChangeSet(changeSet); err != nil {
		if err == errDupChange {
			return nil
		}
		return err
	}
	oldL0s := shard.loadL0Tables()
	newL0Tbls := &l0Tables{make([]*sstable.L0Table, 0, len(oldL0s.tables)*2)}
	del := &deletions{resources: map[uint64]epoch.Resource{}}
	for _, l0 := range splitFiles.L0Creates {
		filename := sstable.NewFilename(l0.ID, sdb.opt.Dir)
		tbl, err := sstable.OpenL0Table(filename, l0.ID)
		if err != nil {
			return err
		}
		newL0Tbls.tables = append(newL0Tbls.tables, tbl)
	}
	for _, oldL0 := range oldL0s.tables {
		if containsUint64(splitFiles.TableDeletes, oldL0.ID()) {
			del.add(oldL0.ID(), oldL0)
		} else {
			newL0Tbls.tables = append(newL0Tbls.tables, oldL0)
		}
	}
	sort.Slice(newL0Tbls.tables, func(i, j int) bool {
		return newL0Tbls.tables[i].CommitTS() > newL0Tbls.tables[j].CommitTS()
	})
	y.Assert(atomic.CompareAndSwapPointer(shard.l0s, unsafe.Pointer(oldL0s), unsafe.Pointer(newL0Tbls)))
	newHandlers := make([][]*levelHandler, sdb.numCFs)
	for cf := 0; cf < sdb.numCFs; cf++ {
		newHandlers[cf] = make([]*levelHandler, ShardMaxLevel)
	}
	for _, tbl := range splitFiles.TableCreates {
		newHandler := newHandlers[tbl.CF][tbl.Level-1]
		if newHandler == nil {
			newHandler = newLevelHandler(sdb.opt.NumLevelZeroTablesStall, int(tbl.Level), sdb.metrics)
			newHandlers[tbl.CF][tbl.Level-1] = newHandler
		}
		filename := sstable.NewFilename(tbl.ID, sdb.opt.Dir)
		reader, err := newTableFile(filename, sdb)
		if err != nil {
			return err
		}
		tbl, err := sstable.OpenTable(reader, sdb.blkCache)
		if err != nil {
			return err
		}
		newHandler.totalSize += tbl.Size()
		newHandler.tables = append(newHandler.tables, tbl)
	}
	for cf := 0; cf < sdb.numCFs; cf++ {
		for level := 1; level <= ShardMaxLevel; level++ {
			newHandler := newHandlers[cf][level-1]
			if newHandler == nil {
				continue
			}
			oldHandler := shard.cfs[cf].getLevelHandler(level)
			for _, oldTbl := range oldHandler.tables {
				if containsUint64(splitFiles.TableDeletes, oldTbl.ID()) {
					del.add(oldTbl.ID(), oldTbl)
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
	shard.setSplitState(changeSet.State)
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

// CompactTables compacts tables in CompactDef and returns the file names.
func CompactTables(cd *CompactDef, stats *y.CompactionStats, discardStats *DiscardStats, s3c *s3util.S3Client) ([]*sstable.BuildResult, error) {
	var buildResults []*sstable.BuildResult
	it := cd.buildIterator()
	defer it.Close()

	skippedTbls := cd.SkippedTbls

	var lastKey, skipKey []byte
	var builder *sstable.Builder
	for it.Valid() {
		var fd *os.File
		var filename string
		var id uint64
		if cd.AllocIDFunc != nil {
			id = cd.AllocIDFunc()
			filename = sstable.NewFilename(id, cd.Dir)
		}
		if !cd.InMemory {
			var err error
			fd, err = y.OpenSyncedFile(filename, false)
			if err != nil {
				return nil, err
			}
		}
		if builder == nil {
			builder = sstable.NewTableBuilder(id, cd.Opt)
		} else {
			builder.Reset(id)
		}
		lastKey = lastKey[:0]
		for ; it.Valid(); table.NextAllVersion(it) {
			stats.KeysRead++
			vs := it.Value()
			key := it.Key()
			kvSize := int(vs.EncodedSize()) + len(key)
			stats.BytesRead += kvSize
			// See if we need to skip this key.
			if len(skipKey) > 0 {
				if bytes.Equal(key, skipKey) {
					discardStats.collect(vs)
					continue
				} else {
					skipKey = skipKey[:0]
				}
			}
			if !bytes.Equal(key, lastKey) {
				// Only break if we are on a different key, and have reached capacity. We want
				// to ensure that all versions of the key are stored in the same sstable, and
				// not divided across multiple tables at the same level.
				if len(skippedTbls) > 0 {
					var over bool
					skippedTbls, over = overSkipTables(key, skippedTbls)
					if over && !builder.Empty() {
						break
					}
				}
				if shouldFinishFile(lastKey, int64(builder.EstimateSize()+kvSize), cd.Opt.MaxTableSize) {
					break
				}
				lastKey = append(lastKey[:0], key...)
			}

			// Only consider the versions which are below the minReadTs, otherwise, we might end up discarding the
			// only valid version for a running transaction.
			if vs.Version <= cd.SafeTS {
				// key is the latest readable version of this key, so we simply discard all the rest of the versions.
				skipKey = append(skipKey[:0], key...)

				if isDeleted(vs.Meta) {
					// If this key range has overlap with lower levels, then keep the deletion
					// marker with the latest version, discarding the rest. We have set skipKey,
					// so the following key versions would be skipped. Otherwise discard the deletion marker.
					if !cd.HasOverlap {
						continue
					}
				} else if cd.Filter != nil {
					switch cd.Filter.Filter(cd.CF, key, vs.Value, vs.UserMeta) {
					case DecisionMarkTombstone:
						discardStats.collect(vs)
						if cd.HasOverlap {
							// There may have ole versions for this key, so convert to delete tombstone.
							builder.Add(key, &y.ValueStruct{Meta: bitDelete, Version: vs.Version})
						}
						continue
					case DecisionDrop:
						discardStats.collect(vs)
						continue
					case DecisionKeep:
					}
				}
			}
			builder.Add(key, &vs)
			stats.KeysWrite++
			stats.BytesWrite += kvSize
		}
		if builder.Empty() {
			continue
		}
		var w io.Writer = fd
		if fd == nil {
			w = bytes.NewBuffer(make([]byte, 0, builder.EstimateSize()))
		}
		result, err := builder.Finish(filename, w)
		if err != nil {
			return nil, err
		}
		if s3c != nil {
			err = putSSTBuildResultToS3(s3c, result)
			if err != nil {
				return nil, err
			}
		}
		fd.Close()
		buildResults = append(buildResults, result)
	}
	return buildResults, nil
}

func shouldFinishFile(lastKey []byte, currentSize, maxSize int64) bool {
	return len(lastKey) > 0 && currentSize > maxSize
}

func overSkipTables(key []byte, skippedTables []table.Table) (newSkippedTables []table.Table, over bool) {
	var i int
	for i < len(skippedTables) {
		t := skippedTables[i]
		if bytes.Compare(key, t.Biggest()) > 0 {
			i++
		} else {
			break
		}
	}
	return skippedTables[i:], i > 0
}

func putSSTBuildResultToS3(s3c *s3util.S3Client, result *sstable.BuildResult) (err error) {
	fileID, _ := sstable.ParseFileID(result.FileName)
	if len(result.FileData) == 0 {
		result.FileData, err = ioutil.ReadFile(result.FileName)
		if err != nil {
			return
		}
	}
	return s3c.Put(s3c.BlockKey(fileID), result.FileData)
}

func sortTables(tables []table.Table) {
	sort.Slice(tables, func(i, j int) bool {
		return bytes.Compare(tables[i].Smallest(), tables[j].Smallest()) < 0
	})
}
