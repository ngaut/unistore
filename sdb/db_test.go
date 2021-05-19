package sdb

import (
	"bytes"
	"fmt"
	"github.com/ngaut/unistore/sdbpb"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
)

func getTestOptions(dir string) Options {
	opt := DefaultOpt
	opt.TableBuilderOptions.MaxTableSize = 4 << 15 // Force more compaction.
	opt.MaxMemTableSize = 4 << 15                  // Force more compaction.
	opt.LevelOneSize = 4 << 15                     // Force more compaction.
	opt.Dir = dir
	return opt
}

func TestDB(t *testing.T) {
	runPprof()
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.NumCompactors = 1
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: true}, {Managed: false}, {Managed: false}}
	db, err := OpenDB(opts)
	require.NoError(t, err)
	initialIngest(t, db)
	sc := &shardingCase{
		t:      t,
		tester: newShardTester(db),
	}
	ch := make(chan time.Duration, 1)
	go func() {
		time.Sleep(time.Millisecond * 100)
		begin := time.Now()
		ver := uint64(1)
		for i := 1000; i < 10000; i += 3000 {
			sc.preSplit(1, ver, iToKey(i))
			sc.finishSplit(1, ver, []uint64{uint64(i), 1})
			ver += 1
		}
		ch <- time.Since(begin)
	}()
	begin := time.Now()
	sc.loadData(0, 10000)
	log.S().Infof("time split %v; load %v", <-ch, time.Since(begin))
	sc.checkData(0, 10000)
	err = db.Close()
	require.NoError(t, err)
}

type shardingCase struct {
	t      *testing.T
	tester *shardTester
}

func iToKey(i int) []byte {
	return []byte(fmt.Sprintf("key%06d", i))
}

func (sc *shardingCase) loadData(begin, end int) {
	var entries []*testerEntry
	for i := begin; i < end; i++ {
		key := iToKey(i)
		entries = append(entries, &testerEntry{
			cf:  0,
			key: key,
			val: key,
			ver: 1,
		}, &testerEntry{
			cf:  1,
			key: key,
			val: bytes.Repeat(key, 2),
		})
		if i%100 == 99 {
			err := sc.tester.write(entries...)
			require.NoError(sc.t, err)
			entries = nil
		}
	}
	if len(entries) > 0 {
		err := sc.tester.write(entries...)
		require.NoError(sc.t, err)
	}
}

func initialIngest(t *testing.T, db *DB) {
	err := db.Ingest(&IngestTree{
		ChangeSet: &sdbpb.ChangeSet{
			ShardID:  1,
			ShardVer: 1,
			Snapshot: &sdbpb.Snapshot{
				Start:      nil,
				End:        globalShardEndKey,
				Properties: &sdbpb.Properties{ShardID: 1},
			},
		},
	})
	require.NoError(t, err)
}

func (sc *shardingCase) checkGet(snap *Snapshot, begin, end int) {
	for i := begin; i < end; i++ {
		key := iToKey(i)
		item, err := snap.Get(0, key, 2)
		require.Nil(sc.t, err)
		require.Equal(sc.t, string(item.val), string(key))
		item2, err := snap.Get(1, key, 0)
		require.Nil(sc.t, err)
		require.Equal(sc.t, string(item2.val), strings.Repeat(string(key), 2))
	}
}

func (sc *shardingCase) checkIterator(snap *Snapshot, begin, end int) {
	for cf := 0; cf < 2; cf++ {
		iter := snap.NewIterator(cf, false, false)
		i := begin
		for iter.Rewind(); iter.Valid(); iter.Next() {
			key := iToKey(i)
			item := iter.Item()
			require.EqualValues(sc.t, key, item.key)
			require.EqualValues(sc.t, bytes.Repeat(key, int(cf)+1), item.val)
			i++
		}
		require.Equal(sc.t, end, i)
		iter.Close()
	}
}

func (sc *shardingCase) preSplit(shardID, ver uint64, keys ...[]byte) {
	err := sc.tester.preSplit(shardID, ver, keys)
	require.NoError(sc.t, err)
}

func (sc *shardingCase) finishSplit(shardID, ver uint64, newIDs []uint64) {
	newProps := make([]*sdbpb.Properties, len(newIDs))
	for i, newID := range newIDs {
		newProps[i] = &sdbpb.Properties{ShardID: newID}
	}
	err := sc.tester.finishSplit(shardID, ver, newProps)
	require.NoError(sc.t, err)
}

func (sc *shardingCase) checkData(begin, end int) {
	i := begin
	err := sc.tester.iterate(iToKey(begin), iToKey(end), 0, func(key, val []byte) {
		require.Equal(sc.t, string(iToKey(i)), string(key))
		require.Equal(sc.t, string(key), string(val))
		i++
	})
	require.Nil(sc.t, err)
	require.Equal(sc.t, end, i)
	i = begin
	err = sc.tester.iterate(iToKey(begin), iToKey(end), 1, func(key, val []byte) {
		require.Equal(sc.t, string(iToKey(i)), string(key))
		require.Equal(sc.t, strings.Repeat(string(key), 2), string(val))
		i++
	})
	require.Nil(sc.t, err)
	require.Equal(sc.t, end, i)
}

func TestSplitSuggestion(t *testing.T) {
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	alloc := new(localIDAllocator)
	opts := getTestOptions(dir)
	opts.NumCompactors = 2
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: true}, {Managed: false}, {Managed: false}}
	opts.IDAllocator = alloc
	db, err := OpenDB(opts)
	require.NoError(t, err)
	initialIngest(t, db)
	sc := &shardingCase{
		t:      t,
		tester: newShardTester(db),
	}
	sc.loadData(0, 20000)
	time.Sleep(time.Second * 2)
	keys := db.GetSplitSuggestion(1, opts.MaxMemTableSize)
	log.S().Infof("split keys %s", keys)
	require.Greater(t, len(keys), 2)
	require.NoError(t, db.Close())
}

func TestShardingMetaChangeListener(t *testing.T) {
	dir, err := ioutil.TempDir("", "sharding")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opts := getTestOptions(dir)
	opts.NumCompactors = 2
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: false}}
	l := new(metaListener)
	opts.MetaChangeListener = l
	db, err := OpenDB(opts)
	require.NoError(t, err)
	initialIngest(t, db)

	numKeys := 1000
	numVers := 10
	for ver := 0; ver < numVers; ver++ {
		wb := db.NewWriteBatch(db.GetShard(1))
		for j := 0; j < numKeys; j++ {
			key := iToKey(j)
			val := append(iToKey(j), iToKey(ver)...)
			require.NoError(t, wb.Put(0, key, y.ValueStruct{Value: val}))
		}
		err := db.Write(wb)
		require.NoError(t, err)
	}
	allCommitTS := l.getAllCommitTS()
	require.True(t, len(allCommitTS) > 0)
	snap := db.NewSnapshot(db.GetShard(1))
	allVals := map[string]struct{}{}
	for i := 0; i < numKeys; i++ {
		key := iToKey(i)
		for _, commitTS := range allCommitTS {
			item, _ := snap.Get(0, key, commitTS)
			if item == nil {
				continue
			}
			allVals[string(item.val)] = struct{}{}
		}
	}
	log.S().Info("all values ", len(allVals))
	// With old version, we can get the old values, so the number of all values is greater than number of keys.
	require.Greater(t, len(allVals), numKeys)
	db.Close()
}

func TestMigration(t *testing.T) {
	t.Skip("the files in local path may be deleted")
	dirA, err := ioutil.TempDir("", "shardingA")
	require.NoError(t, err)
	dirB, err := ioutil.TempDir("", "shardingB")
	require.NoError(t, err)
	defer func() {
		os.RemoveAll(dirA)
		os.RemoveAll(dirB)
	}()
	allocator := &localIDAllocator{}
	opts := getTestOptions(dirA)
	opts.IDAllocator = allocator
	opts.NumCompactors = 2
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: true}, {Managed: false}}
	db, err := OpenDB(opts)
	require.NoError(t, err)
	initialIngest(t, db)
	sc := &shardingCase{
		t:      t,
		tester: newShardTester(db),
	}
	sc.loadData(0, 5000)
	sc.preSplit(1, 1, iToKey(2500))
	sc.finishSplit(1, 1, []uint64{uint64(2), 1})
	end := 5899
	sc.loadData(5000, end)
	time.Sleep(time.Millisecond * 100)
	ingestTree := &IngestTree{MaxTS: db.orc.readTs(), LocalPath: dirA}
	ingestTree.ChangeSet, err = db.manifest.toChangeSet(1)
	opts = getTestOptions(dirB)
	opts.IDAllocator = allocator
	opts.NumCompactors = 2
	opts.NumLevelZeroTables = 1
	opts.CFs = []CFConfig{{Managed: true}, {Managed: false}}
	dbB, err := OpenDB(opts)
	require.Nil(t, err)
	err = dbB.Ingest(ingestTree)
	require.Nil(t, err)
	readTS := dbB.orc.readTs()
	require.True(t, readTS == ingestTree.MaxTS)
	scB := &shardingCase{
		t:      t,
		tester: newShardTester(dbB),
	}
	scB.checkData(2500, 5500)
}

type metaListener struct {
	lock        sync.Mutex
	allCommitTS []uint64
	maxCommitTS uint64
}

func (l *metaListener) OnChange(e *sdbpb.ChangeSet) {
	l.lock.Lock()
	if e.Flush != nil {
		l.allCommitTS = append(l.allCommitTS, e.Flush.CommitTS)
		if e.Flush.CommitTS > l.maxCommitTS {
			l.maxCommitTS = e.Flush.CommitTS
		}
	}
	l.lock.Unlock()
}

func (l *metaListener) getAllCommitTS() []uint64 {
	var result []uint64
	l.lock.Lock()
	result = append(result, l.allCommitTS...)
	l.lock.Unlock()
	return result
}

var once = sync.Once{}

func runPprof() {
	once.Do(func() {
		go func() {
			http.ListenAndServe(":9291", nil)
		}()
	})
}
