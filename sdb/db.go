package sdb

import (
	"fmt"
	"github.com/ngaut/unistore/s3util"
	"github.com/ngaut/unistore/sdb/cache"
	"github.com/ngaut/unistore/sdb/epoch"
	"github.com/ngaut/unistore/sdb/table/memtable"
	"github.com/ngaut/unistore/sdb/table/sstable"
	"github.com/ngaut/unistore/sdbpb"
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

type closers struct {
	updateSize      *y.Closer
	compactors      *y.Closer
	resourceManager *y.Closer
	blobManager     *y.Closer
	memtable        *y.Closer
	s3Client        *y.Closer
}

type DB struct {
	opt           Options
	numCFs        int
	dirLock       *directoryLockGuard
	shardMap      sync.Map
	blkCache      *cache.Cache
	resourceMgr   *epoch.ResourceManager
	safeTsTracker safeTsTracker
	closers       closers
	flushCh       chan *flushTask
	metrics       *y.MetricsSet
	manifest      *Manifest
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

func OpenDB(opt Options) (db *DB, err error) {
	log.Info("Open DB")
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
	manifest, err := OpenManifest(opt.Dir)
	if err != nil {
		return nil, err
	}
	blkCache, err := createCache(opt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create block cache")
	}
	metrics := y.NewMetricSet(opt.Dir)
	db = &DB{
		opt:                opt,
		numCFs:             len(opt.CFs),
		dirLock:            dirLockGuard,
		metrics:            metrics,
		blkCache:           blkCache,
		flushCh:            make(chan *flushTask, opt.NumMemtables),
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
	db.closers.s3Client = y.NewCloser(0)
	if opt.S3Options.EndPoint != "" {
		db.s3c = s3util.NewS3Client(db.closers.s3Client, opt.Dir, opt.S3Options)
	}
	if err = db.loadShards(); err != nil {
		return nil, errors.AddStack(err)
	}
	db.closers.memtable = y.NewCloser(1)
	go db.runFlushMemTable(db.closers.memtable)
	if !db.opt.DoNotCompact {
		db.closers.compactors = y.NewCloser(1)
		go db.runCompactionLoop(db.closers.compactors)
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

func createCache(opt Options) (blkCache *cache.Cache, err error) {
	if opt.MaxBlockCacheSize != 0 {
		blkCache, err = cache.NewCache(&cache.Config{
			// The expected keys is MaxCacheSize / BlockSize, then x10 as documentation suggests.
			NumCounters: opt.MaxBlockCacheSize / int64(opt.TableBuilderOptions.BlockSize) * 10,
			MaxCost:     opt.MaxBlockCacheSize,
			BufferItems: 64,
			OnEvict:     sstable.OnEvict,
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to create block cache")
		}
	}
	return
}

func (sdb *DB) DebugHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Time %s\n", time.Now().Format(time.RFC3339Nano))
		fmt.Fprintf(w, "Manifest.shards %s\n", formatInt(len(sdb.manifest.shards)))
		fmt.Fprintf(w, "Manifest.globalFiles %s\n", formatInt(len(sdb.manifest.globalFiles)))
		fmt.Fprintf(w, "FlushCh %s\n", formatInt(len(sdb.flushCh)))
		MemTables := 0
		MemTablesSize := 0
		L0Tables := 0
		L0TablesSize := 0
		CFs := 0
		LevelHandlers := 0
		CFsSize := 0
		LNTables := 0
		LNTablesSize := 0
		type shardStat struct {
			key           uint64
			ShardSize     int
			MemTablesSize int
			L0TablesSize  int
			CFsSize       int
			CFSize        [3]int
		}
		list := []shardStat{}
		sdb.shardMap.Range(func(key, value interface{}) bool {
			k := key.(uint64)
			shard := value.(*Shard)
			memTables := shard.loadMemTables()
			l0Tables := shard.loadL0Tables()
			MemTables += len(memTables.tables)
			ShardMemTablesSize := 0
			for _, t := range memTables.tables {
				ShardMemTablesSize += int(t.Size())
			}
			MemTablesSize += ShardMemTablesSize
			L0Tables += len(l0Tables.tables)
			ShardL0TablesSize := 0
			for _, t := range l0Tables.tables {
				ShardL0TablesSize += int(t.Size())
			}
			L0TablesSize += ShardL0TablesSize
			CFs += len(shard.cfs)
			ShardCFsSize := 0
			CFSize := [3]int{}
			for i, cf := range shard.cfs {
				LevelHandlers += len(cf.levels)
				for l := range cf.levels {
					level := cf.getLevelHandler(l + 1)
					CFSize[i] += int(level.totalSize)
					LNTables += len(level.tables)
					for _, t := range level.tables {
						LNTablesSize += int(t.Size())
					}
				}
				ShardCFsSize += CFSize[i]
			}
			CFsSize += ShardCFsSize
			stat := shardStat{
				key:           k,
				ShardSize:     ShardMemTablesSize + ShardL0TablesSize + ShardCFsSize,
				MemTablesSize: ShardMemTablesSize,
				L0TablesSize:  ShardL0TablesSize,
				CFsSize:       ShardCFsSize,
				CFSize:        CFSize,
			}
			list = append(list, stat)
			return true
		})
		fmt.Fprintf(w, "MemTables %s, MemTablesSize %s\n", formatInt(MemTables), formatInt(MemTablesSize))
		fmt.Fprintf(w, "L0Tables %s, L0TablesSize %s\n", formatInt(L0Tables), formatInt(L0TablesSize))
		fmt.Fprintf(w, "CFs %s, LevelHandlers %s, LNTables %s, CFsSize %s, LNTablesSize %s\n",
			formatInt(CFs),
			formatInt(LevelHandlers),
			formatInt(LNTables),
			formatInt(CFsSize),
			formatInt(LNTablesSize),
		)
		fmt.Fprintf(w, "Size %s\n", formatInt(MemTablesSize+L0TablesSize+CFsSize))
		fmt.Fprintf(w, "ShardMap %s\n", formatInt(len(list)))
		sort.Slice(list, func(i, j int) bool {
			return list[i].ShardSize > list[j].ShardSize
		})
		for _, shardStat := range list {
			key := shardStat.key
			if value, ok := sdb.shardMap.Load(key); ok {
				shard := value.(*Shard)
				memTables := shard.loadMemTables()
				l0Tables := shard.loadL0Tables()
				if r.FormValue("detail") == "" {
					fmt.Fprintf(w, "\tShard\t%d:%d,\tSize % 13s,\tMem % 13s,\tL0 % 13s,\tCF0 % 13s,\tCF1 % 13s,\tStage % 20s\n\n",
						key,
						shard.Ver,
						formatInt(shardStat.ShardSize),
						formatInt(shardStat.MemTablesSize),
						formatInt(shardStat.L0TablesSize),
						formatInt(shardStat.CFSize[0]),
						formatInt(shardStat.CFSize[1]),
						sdbpb.SplitStage_name[shard.splitStage],
					)
					continue
				}
				fmt.Fprintf(w, "\tShard %d:%d, Size %s, Stage %s\n",
					key,
					shard.Ver,
					formatInt(shardStat.ShardSize),
					sdbpb.SplitStage_name[shard.splitStage],
				)
				fmt.Fprintf(w, "\t\tMemTables %d,  Size %s\n", len(memTables.tables), formatInt(shardStat.MemTablesSize))
				for i, t := range memTables.tables {
					if !t.Empty() {
						fmt.Fprintf(w, "\t\t\tMemTable %d, Size %s\n", i, formatInt(int(t.Size())))
					}
				}
				fmt.Fprintf(w, "\t\tL0Tables %d,  Size %s\n", len(l0Tables.tables), formatInt(shardStat.L0TablesSize))
				for i, t := range l0Tables.tables {
					fmt.Fprintf(w, "\t\t\tL0Table %d, fid %d, size %s \n", i, t.ID(), formatInt(int(t.Size())))
				}
				fmt.Fprintf(w, "\t\tCFs Size %s\n", formatInt(shardStat.CFsSize))
				if shardStat.CFsSize > 0 {
					for i, cf := range shard.cfs {
						fmt.Fprintf(w, "\t\t\tCF %d, Size %s\n", i, formatInt(shardStat.CFSize[i]))
						if shardStat.CFSize[i] > 0 {
							for l := range cf.levels {
								level := cf.getLevelHandler(l + 1)
								fmt.Fprintf(w, "\t\t\t\tlevel %d, tables %s, totalSize %s \n",
									level.level,
									formatInt(len(level.tables)),
									formatInt(int(level.totalSize)),
								)
							}
						}

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

func (sdb *DB) loadShards() error {
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
	return nil
}

func (sdb *DB) loadShard(shardInfo *ShardMeta) (*Shard, error) {
	shard := newShardForLoading(shardInfo, &sdb.opt, sdb.metrics)
	for fid := range shardInfo.files {
		fileMeta, ok := sdb.manifest.globalFiles[fid]
		y.AssertTruef(ok, "%d:%d global file %d not found", shardInfo.ID, shardInfo.Ver, fid)
		cf := fileMeta.cf
		if cf == -1 {
			filename := sstable.NewFilename(fid, sdb.opt.Dir)
			sl0Tbl, err := sstable.OpenL0Table(filename, fid, fileMeta.smallest, fileMeta.biggest)
			if err != nil {
				return nil, err
			}
			shard.addEstimatedSize(sl0Tbl.Size())
			l0Tbls := shard.loadL0Tables()
			l0Tbls.tables = append(l0Tbls.tables, sl0Tbl)
			continue
		}
		level := fileMeta.level
		scf := shard.cfs[cf]
		handler := scf.getLevelHandler(int(level))
		filename := sstable.NewFilename(fid, sdb.opt.Dir)
		reader, err := newTableFile(filename, sdb)
		if err != nil {
			return nil, err
		}
		tbl, err := sstable.OpenTable(reader, sdb.blkCache)
		if err != nil {
			return nil, err
		}
		shard.addEstimatedSize(tbl.Size())
		handler.totalSize += tbl.Size()
		handler.tables = append(handler.tables, tbl)
	}
	l0Tbls := shard.loadL0Tables()
	// Sort the l0 tables by age.
	sort.Slice(l0Tbls.tables, func(i, j int) bool {
		return l0Tbls.tables[i].CommitTS() > l0Tbls.tables[j].CommitTS()
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

func newTableFile(filename string, sdb *DB) (sstable.TableFile, error) {
	reader, err := sstable.NewLocalFile(filename, sdb.blkCache == nil)
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
	Recover(db *DB, shard *Shard, info *ShardMeta, toState *sdbpb.Properties) error
}

type localIDAllocator struct {
	latest uint64
}

func (l *localIDAllocator) AllocID() uint64 {
	return atomic.AddUint64(&l.latest, 1)
}

func (sdb *DB) Close() error {
	atomic.StoreUint32(&sdb.closed, 1)
	log.S().Info("closing DB")
	close(sdb.flushCh)
	sdb.closers.memtable.SignalAndWait()
	if !sdb.opt.DoNotCompact {
		sdb.closers.compactors.SignalAndWait()
	}
	sdb.closers.resourceManager.SignalAndWait()
	if sdb.opt.S3Options.EndPoint != "" {
		sdb.closers.s3Client.SignalAndWait()
	}
	return sdb.dirLock.release()
}

type WriteBatch struct {
	shard         *Shard
	cfConfs       []CFConfig
	entries       [][]*memtable.Entry
	estimatedSize int64
	properties    map[string][]byte
}

func (sdb *DB) NewWriteBatch(shard *Shard) *WriteBatch {
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

type Snapshot struct {
	guard *epoch.Guard
	shard *Shard
	cfs   []CFConfig

	managedReadTS uint64
}

func (s *Snapshot) Get(cf int, key []byte, version uint64) (*Item, error) {
	if version == 0 {
		version = math.MaxUint64
	}
	vs := s.shard.Get(cf, key, version)
	if !vs.Valid() {
		return nil, ErrKeyNotFound
	}
	if isDeleted(vs.Meta) {
		return nil, ErrKeyNotFound
	}
	item := new(Item)
	item.key = key
	item.ver = vs.Version
	item.meta = vs.Meta
	item.userMeta = vs.UserMeta
	item.val = vs.Value
	return item, nil
}

func (s *Snapshot) MultiGet(cf int, keys [][]byte, version uint64) ([]*Item, error) {
	if version == 0 {
		version = math.MaxUint64
	}
	items := make([]*Item, len(keys))
	for i, key := range keys {
		item, err := s.Get(cf, key, version)
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

func (sdb *DB) NewSnapshot(shard *Shard) *Snapshot {
	guard := sdb.resourceMgr.Acquire()
	return &Snapshot{
		guard: guard,
		shard: shard,
		cfs:   sdb.opt.CFs,
	}
}

func (sdb *DB) RemoveShard(shardID uint64, removeFile bool) error {
	shardVal, ok := sdb.shardMap.Load(shardID)
	if !ok {
		return errors.New("shard not found")
	}
	shard := shardVal.(*Shard)
	change := newChangeSet(shard)
	change.ShardDelete = true
	err := sdb.manifest.writeChangeSet(change)
	if err != nil {
		return err
	}
	shard.removeFilesOnDel = removeFile
	sdb.shardMap.Delete(shardID)
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()
	guard.Delete([]epoch.Resource{&deletion{res: shard, delete: func() {
		shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
			for _, tbl := range level.tables {
				if shard.removeFilesOnDel {
					if sdb.s3c != nil {
						sdb.s3c.SetExpired(tbl.ID())
					}
				}
			}
			return false
		})
	}}})
	return nil
}

func (sdb *DB) GetShard(shardID uint64) *Shard {
	shardVal, ok := sdb.shardMap.Load(shardID)
	if !ok {
		return nil
	}
	return shardVal.(*Shard)
}

func (sdb *DB) GetSplitSuggestion(shardID uint64, splitSize int64) [][]byte {
	shard := sdb.GetShard(shardID)
	var keys [][]byte
	if atomic.LoadInt64(&shard.estimatedSize) > splitSize {
		log.S().Infof("shard(%x, %x) size %d", shard.Start, shard.End, shard.estimatedSize)
		keys = append(keys, shard.getSuggestSplitKeys(splitSize)...)
	}
	return keys
}

func (sdb *DB) Size() int64 {
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

func (sdb *DB) NumCFs() int {
	return sdb.numCFs
}

func (sdb *DB) GetOpt() Options {
	return sdb.opt
}

func (sdb *DB) GetShardChangeSet(shardID uint64) (*sdbpb.ChangeSet, error) {
	sdb.manifest.appendLock.Lock()
	defer sdb.manifest.appendLock.Unlock()
	return sdb.manifest.toChangeSet(shardID)
}

func (sdb *DB) TriggerFlush(shard *Shard, skipCnt int) {
	mems := shard.loadMemTables()
	for i := len(mems.tables) - skipCnt - 1; i > 0; i-- {
		memTbl := mems.tables[i]
		sdb.flushCh <- &flushTask{
			shard: shard,
			tbl:   memTbl,
		}
	}
	if len(mems.tables) == 1 && mems.tables[0].Empty() {
		if !shard.IsInitialFlushed() {
			commitTS := shard.allocCommitTS()
			memTbl := memtable.NewCFTable(sdb.numCFs)
			memTbl.SetVersion(commitTS)
			sdb.flushCh <- &flushTask{
				shard: shard,
				tbl:   memTbl,
			}
		}
	}
}
