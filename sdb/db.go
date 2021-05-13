package sdb

import (
	"fmt"
	"github.com/pingcap/badger/cache"
	"github.com/pingcap/badger/epoch"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/s3util"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"math"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errShardNotFound            = errors.New("shard not found")
	errShardNotMatch            = errors.New("shard not match")
	errShardWrongSplittingState = errors.New("shard wrong splitting state")
)

type closers struct {
	updateSize      *y.Closer
	compactors      *y.Closer
	resourceManager *y.Closer
	blobManager     *y.Closer
	memtable        *y.Closer
	writes          *y.Closer
}

type ShardingDB struct {
	opt           Options
	numCFs        int
	orc           *oracle
	dirLock       *directoryLockGuard
	shardMap      sync.Map
	blkCache      *cache.Cache
	idxCache      *cache.Cache
	resourceMgr   *epoch.ResourceManager
	safeTsTracker safeTsTracker
	closers       closers
	writeCh       chan engineTask
	flushCh       chan *shardFlushTask
	metrics       *y.MetricsSet
	manifest      *ShardingManifest
	mangedSafeTS  uint64
	idAlloc       IDAllocator
	s3c           *s3util.S3Client
	closed        uint32

	metaChangeListener MetaChangeListener
}

const (
	kvWriteChCapacity = 1000
	lockFile          = "LOCK"
)

func OpenShardingDB(opt Options) (db *ShardingDB, err error) {
	log.Info("Open sharding DB")
	err = checkOptions(&opt)
	if err != nil {
		return nil, err
	}
	var dirLockGuard *directoryLockGuard
	dirLockGuard, err = acquireDirectoryLock(opt.Dir, lockFile)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = dirLockGuard.release()
		}
	}()
	manifest, err := OpenShardingManifest(opt.Dir)
	if err != nil {
		return nil, err
	}

	orc := &oracle{
		curRead:    manifest.dataVersion,
		nextCommit: manifest.dataVersion + 1,
		commits:    make(map[uint64]uint64),
	}
	manifest.orc = orc
	blkCache, idxCache, err := createCache(opt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create block cache")
	}
	metrics := y.NewMetricSet(opt.Dir)
	db = &ShardingDB{
		opt:                opt,
		numCFs:             len(opt.CFs),
		orc:                orc,
		dirLock:            dirLockGuard,
		metrics:            metrics,
		blkCache:           blkCache,
		idxCache:           idxCache,
		flushCh:            make(chan *shardFlushTask, opt.NumMemtables),
		writeCh:            make(chan engineTask, kvWriteChCapacity),
		manifest:           manifest,
		metaChangeListener: opt.MetaChangeListener,
	}
	if opt.IDAllocator != nil {
		db.idAlloc = opt.IDAllocator
	} else {
		db.idAlloc = &localIDAllocator{latest: manifest.lastID}
	}
	db.closers.resourceManager = y.NewCloser(0)
	db.resourceMgr = epoch.NewResourceManager(db.closers.resourceManager, &db.safeTsTracker)
	if opt.S3Options.EndPoint != "" {
		db.s3c = s3util.NewS3Client(opt.S3Options)
	}
	if err = db.loadShards(); err != nil {
		return nil, errors.AddStack(err)
	}
	db.closers.memtable = y.NewCloser(1)
	go db.runFlushMemTable(db.closers.memtable)
	db.closers.writes = y.NewCloser(1)
	go db.runWriteLoop(db.closers.writes)
	if !db.opt.DoNotCompact {
		db.closers.compactors = y.NewCloser(1)
		go db.runShardInternalCompactionLoop(db.closers.compactors)
	}
	return db, nil
}

func checkOptions(opt *Options) error {
	path := opt.Dir
	dirExists, err := exists(path)
	if err != nil {
		return y.Wrapf(err, "Invalid Dir: %q", path)
	}
	if !dirExists {
		// Try to create the directory
		err = os.Mkdir(path, 0700)
		if err != nil {
			return y.Wrapf(err, "Error Creating Dir: %q", path)
		}
	}
	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func createCache(opt Options) (blkCache, idxCache *cache.Cache, err error) {
	if opt.MaxBlockCacheSize != 0 {
		blkCache, err = cache.NewCache(&cache.Config{
			// The expected keys is MaxCacheSize / BlockSize, then x10 as documentation suggests.
			NumCounters: opt.MaxBlockCacheSize / int64(opt.TableBuilderOptions.BlockSize) * 10,
			MaxCost:     opt.MaxBlockCacheSize,
			BufferItems: 64,
			OnEvict:     sstable.OnEvict,
		})
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to create block cache")
		}

		indexSizeHint := float64(opt.TableBuilderOptions.MaxTableSize) / 6.0
		idxCache, err = cache.NewCache(&cache.Config{
			NumCounters: int64(float64(opt.MaxIndexCacheSize) / indexSizeHint * 10),
			MaxCost:     opt.MaxIndexCacheSize,
			BufferItems: 64,
		})
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to create index cache")
		}
	}
	return
}

func (sdb *ShardingDB) DebugHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Time %s\n", time.Now().Format(time.RFC3339Nano))
		fmt.Fprintf(w, "Manifest.shards %s\n", formatInt(len(sdb.manifest.shards)))
		fmt.Fprintf(w, "Manifest.globalFiles %s\n", formatInt(len(sdb.manifest.globalFiles)))
		keys := []int{}
		MemTables := 0
		MemTablesSize := 0
		L0Tables := 0
		L0TablesSize := 0
		CFs := 0
		Levels := 0
		CFsSize := 0
		sdb.shardMap.Range(func(key, value interface{}) bool {
			keys = append(keys, int(key.(uint64)))
			shard := value.(*Shard)
			memTables := shard.loadMemTables()
			l0Tables := shard.loadL0Tables()
			MemTables += len(memTables.tables)
			for _, t := range memTables.tables {
				MemTablesSize += int(t.Size())
			}
			L0Tables += len(l0Tables.tables)
			for _, t := range l0Tables.tables {
				L0TablesSize += int(t.size)
			}
			CFs += len(shard.cfs)
			for _, cf := range shard.cfs {
				Levels += len(cf.levels)
				for l := range cf.levels {
					level := cf.getLevelHandler(l + 1)
					CFsSize += int(level.totalSize)
				}
			}
			return true
		})
		fmt.Fprintf(w, "MemTables %d, MemTablesSize %s\n", MemTables, formatInt(MemTablesSize))
		fmt.Fprintf(w, "L0Tables %d, L0TablesSize %s\n", L0Tables, formatInt(L0TablesSize))
		fmt.Fprintf(w, "CFs %d, Levels %d, CFsSize %s\n", CFs, Levels, formatInt(CFsSize))
		fmt.Fprintf(w, "ShardMap %d\n", len(keys))
		sort.Ints(keys)
		for _, k := range keys {
			key := uint64(k)
			if value, ok := sdb.shardMap.Load(key); ok {
				shard := value.(*Shard)
				memTables := shard.loadMemTables()
				l0Tables := shard.loadL0Tables()
				fmt.Fprintf(w, "\tShard ID %d, Version %d, SplitState %s\n", key, shard.Ver, protos.SplitState_name[shard.splitState])
				fmt.Fprintf(w, "\t\tMemTables %d\n", len(memTables.tables))
				for i, t := range memTables.tables {
					fmt.Fprintf(w, "\t\t\tMemTable %d, Size %s, Empty %t \n", i, formatInt(int(t.Size())), t.Empty())
				}
				fmt.Fprintf(w, "\t\tL0Tables %d\n", len(l0Tables.tables))
				for i, t := range l0Tables.tables {
					fmt.Fprintf(w, "\t\t\tL0Table %d, fid %d, cfs %d, size %s \n", i, t.fid, len(t.cfs), formatInt(int(t.size)))
				}
				fmt.Fprintf(w, "\t\tcfs %d\n", len(shard.cfs))
				for i, cf := range shard.cfs {
					fmt.Fprintf(w, "\t\t\tCF %d, levels %d \n", i, len(cf.levels))
					for l := range cf.levels {
						level := cf.getLevelHandler(l + 1)
						fmt.Fprintf(w, "\t\t\t\tLevelHandler %d, level %d, tables %d, totalSize %s \n", l, level.level, len(level.tables), formatInt(int(level.totalSize)))
					}
				}
			}
		}
	}
}

func formatInt(n int) string {
	str := fmt.Sprintf("%d", n)
	length := len(str)
	if length <= 3 {
		return str
	}
	separators := (length - 1) / 3
	buf := make([]byte, length+separators)
	for i := 0; i < separators; i++ {
		buf[len(buf)-(i+1)*4] = ','
		copy(buf[len(buf)-(i+1)*4+1:], str[length-(i+1)*3:length-i*3])
	}
	copy(buf, str[:length-separators*3])
	return string(buf)
}

func (sdb *ShardingDB) loadShards() error {
	log.Info("before load shards")
	sdb.PrintStructure()
	for _, mShard := range sdb.manifest.shards {
		parent := mShard.parent
		if parent != nil && !parent.recovered && sdb.opt.RecoverHandler != nil {
			parentShard, err := sdb.loadShard(parent)
			if err != nil {
				return errors.AddStack(err)
			}
			err = sdb.opt.RecoverHandler.Recover(sdb, parentShard, parent, parent.split.MemProps)
			if err != nil {
				return errors.AddStack(err)
			}
			parent.recovered = true
		}
		mShard.parent = nil
		shard, err := sdb.loadShard(mShard)
		if err != nil {
			return err
		}
		if sdb.opt.RecoverHandler != nil {
			if mShard.preSplit != nil {
				if mShard.preSplit.MemProps != nil {
					// Recover to the state before PreSplit.
					err = sdb.opt.RecoverHandler.Recover(sdb, shard, mShard, mShard.preSplit.MemProps)
					if err != nil {
						return errors.AddStack(err)
					}
					shard.setSplitKeys(mShard.preSplit.Keys)
				}
			}
			err = sdb.opt.RecoverHandler.Recover(sdb, shard, mShard, nil)
			if err != nil {
				return errors.AddStack(err)
			}
		}
	}
	log.Info("after load shards")
	sdb.PrintStructure()
	return nil
}

func (sdb *ShardingDB) loadShard(shardInfo *ShardMeta) (*Shard, error) {
	shard := newShardForLoading(shardInfo, sdb.opt, sdb.metrics)
	for fid := range shardInfo.files {
		cfLevel, ok := sdb.manifest.globalFiles[fid]
		y.AssertTruef(ok, "%d:%d global file %d not found", shardInfo.ID, shardInfo.Ver, fid)
		cf := cfLevel.cf
		if cf == -1 {
			filename := sstable.NewFilename(fid, sdb.opt.Dir)
			sl0Tbl, err := openShardL0Table(filename, fid)
			if err != nil {
				return nil, err
			}
			shard.addEstimatedSize(sl0Tbl.size)
			l0Tbls := shard.loadL0Tables()
			l0Tbls.tables = append(l0Tbls.tables, sl0Tbl)
			continue
		}
		level := cfLevel.level
		scf := shard.cfs[cf]
		handler := scf.getLevelHandler(int(level))
		filename := sstable.NewFilename(fid, sdb.opt.Dir)
		reader, err := newTableFileWithShardingDB(filename, sdb)
		if err != nil {
			return nil, err
		}
		tbl, err := sstable.OpenTable(filename, reader)
		if err != nil {
			return nil, err
		}
		shard.addEstimatedSize(tbl.Size())
		handler.tables = append(handler.tables, tbl)
	}
	l0Tbls := shard.loadL0Tables()
	// Sort the l0 tables by age.
	sort.Slice(l0Tbls.tables, func(i, j int) bool {
		return l0Tbls.tables[i].commitTS > l0Tbls.tables[j].commitTS
	})
	for cf := 0; cf < len(sdb.opt.CFs); cf++ {
		scf := shard.cfs[cf]
		for level := 1; level <= ShardMaxLevel; level++ {
			handler := scf.getLevelHandler(level)
			sortTables(handler.tables)
		}
	}
	sdb.shardMap.Store(shard.ID, shard)
	log.S().Infof("load shard %d ver %d", shard.ID, shard.Ver)
	return shard, nil
}

func newTableFileWithShardingDB(filename string, sdb *ShardingDB) (sstable.TableFile, error) {
	var reader sstable.TableFile
	var err error
	if sdb.blkCache != nil {
		reader, err = sstable.NewLocalFile(filename, sdb.blkCache, sdb.idxCache)
	} else {
		reader, err = sstable.NewMMapFile(filename)
	}
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// RecoverHandler handles recover a shard's mem-table data from another data source.
type RecoverHandler interface {
	// Recover recovers from the shard's state to the state that is stored in the toState property.
	// So the DB has a chance to execute pre-split command.
	// If toState is nil, the implementation should recovers to the latest state.
	Recover(db *ShardingDB, shard *Shard, info *ShardMeta, toState *protos.ShardProperties) error
}

type localIDAllocator struct {
	latest uint64
}

func (l *localIDAllocator) AllocID() uint64 {
	return atomic.AddUint64(&l.latest, 1)
}

func (sdb *ShardingDB) Close() error {
	atomic.StoreUint32(&sdb.closed, 1)
	log.S().Info("closing ShardingDB")
	sdb.closers.writes.SignalAndWait()
	close(sdb.flushCh)
	sdb.closers.memtable.SignalAndWait()
	if !sdb.opt.DoNotCompact {
		sdb.closers.compactors.SignalAndWait()
	}
	sdb.closers.resourceManager.SignalAndWait()
	return sdb.dirLock.release()
}

func (sdb *ShardingDB) GetSafeTS() (uint64, uint64, uint64) {
	return sdb.orc.readTs(), sdb.orc.commitTs(), atomic.LoadUint64(&sdb.safeTsTracker.safeTs)
}

func (sdb *ShardingDB) PrintStructure() {
	var allShards []*Shard
	sdb.shardMap.Range(func(key, value interface{}) bool {
		allShards = append(allShards, value.(*Shard))
		return true
	})
	var strs []string
	for _, shard := range allShards {
		mems := shard.loadMemTables()
		var memSizes []int64
		for _, mem := range mems.tables {
			memSizes = append(memSizes, mem.Size())
		}
		if len(memSizes) > 0 {
			strs = append(strs, fmt.Sprintf("shard %d mem sizes %v", shard.ID, memSizes))
		}
		l0s := shard.loadL0Tables()
		var l0IDs []uint64
		for _, tbl := range l0s.tables {
			l0IDs = append(l0IDs, tbl.fid)
		}
		shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
			assertTablesOrder(level.level, level.tables, nil)
			for _, tbl := range level.tables {
				y.Assert(shard.OverlapKey(tbl.Smallest().UserKey))
				y.Assert(shard.OverlapKey(tbl.Biggest().UserKey))
			}
			return false
		})
		if len(l0IDs) > 0 {
			strs = append(strs, fmt.Sprintf("shard %d l0 tables %v", shard.ID, l0IDs))
		}
		for cf, scf := range shard.cfs {
			var tableIDs [][]uint64
			var tableCnt int
			for l := 1; l <= ShardMaxLevel; l++ {
				levelTblIDs := getTblIDs(scf.getLevelHandler(l).tables)
				tableIDs = append(tableIDs, levelTblIDs)
				tableCnt += len(levelTblIDs)
			}
			if tableCnt > 0 {
				strs = append(strs, fmt.Sprintf("shard %d cf %d tables %v", shard.ID, cf, tableIDs))
			}
		}
	}
	for id, fi := range sdb.manifest.shards {
		cfs := make([][]uint64, sdb.numCFs)
		l0s := make([]uint64, 0, 10)
		for fid := range fi.files {
			cfLevel, ok := sdb.manifest.globalFiles[fid]
			if !ok {
				log.S().Errorf("shard %d fid %d not found in global", fi.ID, fid)
			}
			if cfLevel.cf == -1 {
				l0s = append(l0s, fid)
			} else {
				cfs[cfLevel.cf] = append(cfs[cfLevel.cf], fid)
			}
		}
		if len(l0s) > 0 {
			strs = append(strs, fmt.Sprintf("manifest shard %d l0 tables %v", id, l0s))
		}
		for cf, cfIDs := range cfs {
			if len(cfIDs) > 0 {
				strs = append(strs, fmt.Sprintf("manifest shard %d cf %d tables %v", id, cf, cfIDs))
			}
		}
	}
	sort.Strings(strs)
	for _, str := range strs {
		log.Info(str)
	}
}

type WriteBatch struct {
	shard         *Shard
	cfConfs       []CFConfig
	entries       [][]*memtable.Entry
	estimatedSize int64
	properties    map[string][]byte
}

func (sdb *ShardingDB) NewWriteBatch(shard *Shard) *WriteBatch {
	return &WriteBatch{
		shard:      shard,
		cfConfs:    sdb.opt.CFs,
		entries:    make([][]*memtable.Entry, sdb.numCFs),
		properties: map[string][]byte{},
	}
}

func (wb *WriteBatch) Put(cf int, key []byte, val y.ValueStruct) error {
	if wb.cfConfs[cf].Managed {
		if val.Version == 0 {
			return fmt.Errorf("version is zero for managed CF")
		}
	} else {
		if val.Version != 0 {
			return fmt.Errorf("version is not zero for non-managed CF")
		}
	}
	wb.entries[cf] = append(wb.entries[cf], &memtable.Entry{
		Key:   key,
		Value: val,
	})
	wb.estimatedSize += int64(len(key) + int(val.EncodedSize()) + memtable.EstimateNodeSize)
	return nil
}

func (wb *WriteBatch) Delete(cf byte, key []byte, version uint64) error {
	if wb.cfConfs[cf].Managed {
		if version == 0 {
			return fmt.Errorf("version is zero for managed CF")
		}
	} else {
		if version != 0 {
			return fmt.Errorf("version is not zero for non-managed CF")
		}
	}
	wb.entries[cf] = append(wb.entries[cf], &memtable.Entry{
		Key:   key,
		Value: y.ValueStruct{Meta: bitDelete, Version: version},
	})
	wb.estimatedSize += int64(len(key) + memtable.EstimateNodeSize)
	return nil
}

func (wb *WriteBatch) SetProperty(key string, val []byte) {
	wb.properties[key] = val
}

func (wb *WriteBatch) EstimatedSize() int64 {
	return wb.estimatedSize
}

func (wb *WriteBatch) NumEntries() int {
	var n int
	for _, entries := range wb.entries {
		n += len(entries)
	}
	return n
}

func (wb *WriteBatch) Reset() {
	for i, entries := range wb.entries {
		wb.entries[i] = entries[:0]
	}
	wb.estimatedSize = 0
	for key := range wb.properties {
		delete(wb.properties, key)
	}
}

func (wb *WriteBatch) Iterate(cf int, fn func(e *memtable.Entry) (more bool)) {
	for _, e := range wb.entries[cf] {
		if !fn(e) {
			break
		}
	}
}

func (sdb *ShardingDB) Write(wbs ...*WriteBatch) error {
	notifies := make([]chan error, len(wbs))
	for i, wb := range wbs {
		notify := make(chan error, 1)
		notifies[i] = notify
		sdb.writeCh <- engineTask{writeTask: wb, notify: notify}
	}
	for _, notify := range notifies {
		err := <-notify
		if err != nil {
			return err
		}
	}
	return nil
}

func (sdb *ShardingDB) RecoverWrite(wb *WriteBatch) error {
	eTask := engineTask{writeTask: wb, notify: make(chan error, 1)}
	sdb.executeWriteTask(eTask)
	return <-eTask.notify
}

type Snapshot struct {
	guard  *epoch.Guard
	readTS uint64
	shard  *Shard
	cfs    []CFConfig

	managedReadTS uint64

	buffer *memtable.CFTable
}

func (s *Snapshot) Get(cf int, key y.Key) (*Item, error) {
	if key.Version == 0 {
		key.Version = s.getDefaultVersion(cf)
	}
	var vs y.ValueStruct
	if s.buffer != nil {
		vs = s.buffer.Get(cf, key.UserKey, key.Version)
	}
	if !vs.Valid() {
		vs = s.shard.Get(cf, key)
	}
	if !vs.Valid() {
		return nil, ErrKeyNotFound
	}
	if isDeleted(vs.Meta) {
		return nil, ErrKeyNotFound
	}
	item := new(Item)
	item.key.UserKey = key.UserKey
	item.key.Version = vs.Version
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.vptr = vs.Value
	return item, nil
}

func (s *Snapshot) getDefaultVersion(cf int) uint64 {
	if s.cfs[cf].Managed {
		return math.MaxUint64
	}
	return s.readTS
}

func (s *Snapshot) MultiGet(cf int, keys [][]byte, version uint64) ([]*Item, error) {
	if version == 0 {
		version = s.getDefaultVersion(cf)
	}
	items := make([]*Item, len(keys))
	for i, key := range keys {
		item, err := s.Get(cf, y.KeyWithTs(key, version))
		if err != nil && err != ErrKeyNotFound {
			return nil, err
		}
		items[i] = item
	}
	return items, nil
}

func (s *Snapshot) Discard() {
	s.guard.Done()
}

func (s *Snapshot) SetManagedReadTS(ts uint64) {
	s.managedReadTS = ts
}

func (s *Snapshot) GetReadTS() uint64 {
	return s.readTS
}

func (s *Snapshot) SetBuffer(buf *memtable.CFTable) {
	s.buffer = buf
}

func (sdb *ShardingDB) NewSnapshot(shard *Shard) *Snapshot {
	readTS := sdb.orc.readTs()
	guard := sdb.resourceMgr.AcquireWithPayload(readTS)
	return &Snapshot{
		guard:  guard,
		shard:  shard,
		readTS: readTS,
		cfs:    sdb.opt.CFs,
	}
}

func (sdb *ShardingDB) GetReadTS() uint64 {
	return sdb.orc.readTs()
}

func (sdb *ShardingDB) RemoveShard(shardID uint64, removeFile bool) error {
	shardVal, ok := sdb.shardMap.Load(shardID)
	if !ok {
		return errors.New("shard not found")
	}
	shard := shardVal.(*Shard)
	change := newShardChangeSet(shard)
	change.ShardDelete = true
	err := sdb.manifest.writeChangeSet(change)
	if err != nil {
		return err
	}
	shard.removeFilesOnDel = removeFile
	sdb.shardMap.Delete(shardID)
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	guard.Delete([]epoch.Resource{shard})
	return nil
}

func (sdb *ShardingDB) GetShard(shardID uint64) *Shard {
	shardVal, ok := sdb.shardMap.Load(shardID)
	if !ok {
		return nil
	}
	return shardVal.(*Shard)
}

func (sdb *ShardingDB) GetSplitSuggestion(shardID uint64, splitSize int64) [][]byte {
	shard := sdb.GetShard(shardID)
	var keys [][]byte
	if atomic.LoadInt64(&shard.estimatedSize) > splitSize {
		log.S().Infof("shard(%x, %x) size %d", shard.Start, shard.End, shard.estimatedSize)
		keys = append(keys, shard.getSuggestSplitKeys(splitSize)...)
	}
	return keys
}

func (sdb *ShardingDB) Size() int64 {
	var size int64
	var shardCnt int64
	sdb.shardMap.Range(func(key, value interface{}) bool {
		shard := value.(*Shard)
		size += atomic.LoadInt64(&shard.estimatedSize)
		shardCnt++
		return true
	})
	return size + shardCnt
}

func (sdb *ShardingDB) NumCFs() int {
	return sdb.numCFs
}

func (sdb *ShardingDB) GetOpt() Options {
	return sdb.opt
}

func (sdb *ShardingDB) GetShardChangeSet(shardID uint64) (*protos.ShardChangeSet, error) {
	sdb.manifest.appendLock.Lock()
	defer sdb.manifest.appendLock.Unlock()
	return sdb.manifest.toChangeSet(shardID)
}

func (sdb *ShardingDB) GetProperties(shard *Shard, keys []string) (values [][]byte) {
	notify := make(chan error, 1)
	task := &getPropertyTask{shard: shard, keys: keys}
	sdb.writeCh <- engineTask{getProperties: task, notify: notify}
	y.Assert(<-notify == nil)
	return task.values
}

func (sdb *ShardingDB) TriggerFlush(shard *Shard) {
	notify := make(chan error, 1)
	task := &triggerFlushTask{shard: shard}
	sdb.writeCh <- engineTask{triggerFlush: task, notify: notify}
	y.Assert(<-notify == nil)
}

// Item is returned during iteration. Both the Key() and Value() output is only valid until
// iterator.Next() is called.
type Item struct {
	err      error
	key      y.Key
	vptr     []byte
	meta     byte // We need to store meta to know about bitValuePointer.
	userMeta []byte
	next     *Item
}

// String returns a string representation of Item
func (item *Item) String() string {
	return fmt.Sprintf("key=%q, version=%d, meta=%x", item.Key(), item.Version(), item.meta)
}

// Key returns the key.
//
// Key is only valid as long as item is valid, or transaction is valid.  If you need to use it
// outside its validity, please use KeyCopy
func (item *Item) Key() []byte {
	return item.key.UserKey
}

// KeyCopy returns a copy of the key of the item, writing it to dst slice.
// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
// returned.
func (item *Item) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, item.key.UserKey)
}

// Version returns the commit timestamp of the item.
func (item *Item) Version() uint64 {
	return item.key.Version
}

// IsEmpty checks if the value is empty.
func (item *Item) IsEmpty() bool {
	return len(item.vptr) == 0
}

// Value retrieves the value of the item from the value log.
//
// This method must be called within a transaction. Calling it outside a
// transaction is considered undefined behavior. If an iterator is being used,
// then Item.Value() is defined in the current iteration only, because items are
// reused.
//
// If you need to use a value outside a transaction, please use Item.ValueCopy
// instead, or copy it yourself. Value might change once discard or commit is called.
// Use ValueCopy if you want to do a Set after Get.
func (item *Item) Value() ([]byte, error) {
	return item.vptr, nil
}

// ValueSize returns the size of the value without the cost of retrieving the value.
func (item *Item) ValueSize() int {
	return len(item.vptr)
}

// ValueCopy returns a copy of the value of the item from the value log, writing it to dst slice.
// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
// returned. Tip: It might make sense to reuse the returned slice as dst argument for the next call.
//
// This function is useful in long running iterate/update transactions to avoid a write deadlock.
// See Github issue: https://github.com/pingcap/badger/issues/315
func (item *Item) ValueCopy(dst []byte) ([]byte, error) {
	buf, err := item.Value()
	if err != nil {
		return nil, err
	}
	return y.SafeCopy(dst, buf), nil
}

func (item *Item) hasValue() bool {
	if item.meta == 0 && item.vptr == nil {
		// key not found
		return false
	}
	return true
}

// IsDeleted returns true if item contains deleted or expired value.
func (item *Item) IsDeleted() bool {
	return isDeleted(item.meta)
}

// EstimatedSize returns approximate size of the key-value pair.
//
// This can be called while iterating through a store to quickly estimate the
// size of a range of key-value pairs (without fetching the corresponding
// values).
func (item *Item) EstimatedSize() int64 {
	if !item.hasValue() {
		return 0
	}
	return int64(item.key.Len() + len(item.vptr))
}

// UserMeta returns the userMeta set by the user. Typically, this byte, optionally set by the user
// is used to interpret the value.
func (item *Item) UserMeta() []byte {
	return item.userMeta
}
