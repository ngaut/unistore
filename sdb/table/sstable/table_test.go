/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/ngaut/unistore/sdb/table"
	"github.com/pingcap/badger/cache"
	"github.com/pingcap/badger/cache/z"
	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/y"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func key(prefix string, i int) string {
	return prefix + fmt.Sprintf("%04d", i)
}

var defaultBuilderOpt = options.TableBuilderOptions{
	SuRFStartLevel: 0,
	SuRFOptions: options.SuRFOptions{
		BitsPerKeyHint: 40,
		RealSuffixLen:  10,
	},
	BlockSize:           4 * 1024,
	HashUtilRatio:       0.75,
	WriteBufferSize:     1024 * 1024,
	MaxLevels:           1,
	LevelSizeMultiplier: 10,
	LogicalBloomFPR:     0.01,
	MaxTableSize:        32 * 1024,
}

func testCache() *cache.Cache {
	c, err := cache.NewCache(&cache.Config{
		NumCounters: 1000000 * 10,
		MaxCost:     1000000,
		BufferItems: 64,
		Metrics:     true,
	})
	y.Check(err)
	return c
}

func generateKeyValues(prefix string, n int) [][]string {
	y.Assert(n <= 10000)
	keyValues := make([][]string, n)
	for i := 0; i < n; i++ {
		k := key(prefix, i)
		v := fmt.Sprintf("%d", i)
		keyValues[i] = []string{k, v}
	}
	return keyValues
}

func buildTestTable(t *testing.T, prefix string, n int) *os.File {
	return buildTable(t, generateKeyValues(prefix, n))
}

// keyValues is n by 2 where n is number of pairs.
func buildTable(t *testing.T, keyValues [][]string) *os.File {
	// TODO: Add test for file garbage collection here. No files should be left after the tests here.
	b, f := newTableBuilderForTest(false)
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for _, kv := range keyValues {
		y.Assert(len(kv) == 2)
		err := b.Add(y.KeyWithTs([]byte(kv[0]), 0), y.ValueStruct{Value: []byte(kv[1]), Meta: 'A', UserMeta: []byte{0}})
		if t != nil {
			require.NoError(t, err)
		} else {
			y.Check(err)
		}
	}
	_, err := b.Finish()
	y.Check(err)
	y.Check(f.Close())
	f, _ = y.OpenSyncedFile(f.Name(), true)
	return f
}

func newTableBuilderForTest(surf bool) (*Builder, *os.File) {
	filename := fmt.Sprintf("%s%s%x.sst", os.TempDir(), string(os.PathSeparator), z.FastRand())
	f, err := y.OpenSyncedFile(filename, true)
	y.Check(err)
	opt := defaultBuilderOpt
	if !surf {
		opt.SuRFStartLevel = 8
	}
	y.Assert(filename == f.Name())
	return NewTableBuilder(f, rate.NewLimiter(rate.Inf, math.MaxInt32), 0, opt), f
}

func buildMultiVersionTable(keyValues [][]string) (*os.File, int) {
	b, f := newTableBuilderForTest(false)
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	allCnt := len(keyValues)
	for _, kv := range keyValues {
		y.Assert(len(kv) == 2)
		val := fmt.Sprintf("%s_%d", kv[1], 9)
		err := b.Add(y.KeyWithTs([]byte(kv[0]), 9), y.ValueStruct{Value: []byte(val), Meta: 'A', UserMeta: []byte{0}})
		y.Check(err)
		for i := uint64(8); i > 0; i-- {
			if z.FastRand()%4 == 0 {
				val = fmt.Sprintf("%s_%d", kv[1], i)
				err = b.Add(y.KeyWithTs([]byte(kv[0]), i), y.ValueStruct{Value: []byte(val), Meta: 'A', UserMeta: []byte{0}})
				y.Check(err)
				allCnt++
			}
		}
	}
	_, err := b.Finish()
	y.Check(err)
	y.Check(f.Close())
	f, _ = y.OpenSyncedFile(f.Name(), true)
	return f, allCnt
}

func TestTableIterator(t *testing.T) {
	for _, n := range []int{99, 100, 101} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTestTable(f.Name(), testCache(), testCache())
			require.NoError(t, err)
			defer table.Delete()
			it := table.newIterator(false)
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				v := it.Value()
				k := y.KeyWithTs([]byte(key("key", count)), 0)
				require.EqualValues(t, k, it.Key())
				require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
				count++
			}
			require.Equal(t, count, n)
			it.Close()
		})
	}
}

func TestHashIndexTS(t *testing.T) {
	filename := fmt.Sprintf("%s%s%x.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
	f, err := y.OpenSyncedFile(filename, true)
	if t != nil {
		require.NoError(t, err)
	} else {
		y.Check(err)
	}
	b := NewTableBuilder(f, nil, 0, defaultBuilderOpt)
	keys := []y.Key{
		y.KeyWithTs([]byte("key"), 9),
		y.KeyWithTs([]byte("key"), 7),
		y.KeyWithTs([]byte("key"), 5),
		y.KeyWithTs([]byte("key"), 3),
		y.KeyWithTs([]byte("key"), 1),
	}
	for _, k := range keys {
		b.Add(k, y.ValueStruct{Value: k.UserKey, Meta: 'A', UserMeta: []byte{0}})
	}
	_, err = b.Finish()
	y.Check(err)
	f.Close()
	table, err := OpenTestTable(filename, testCache(), testCache())
	keyHash := farm.Fingerprint64([]byte("key"))

	rk, _, ok, err := table.pointGet(y.KeyWithTs([]byte("key"), 10), keyHash)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, rk.Equal(keys[0]), "%s", string(rk.UserKey))

	rk, _, ok, err = table.pointGet(y.KeyWithTs([]byte("key"), 6), keyHash)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, rk.Equal(keys[2]))

	rk, _, ok, err = table.pointGet(y.KeyWithTs([]byte("key"), 2), keyHash)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, rk.Equal(keys[4]))
}

func TestPointGet(t *testing.T) {
	f := buildTestTable(t, "key", 8000)
	table, err := OpenTestTable(f.Name(), testCache(), testCache())
	require.NoError(t, err)
	defer table.Delete()

	for i := 0; i < 8000; i++ {
		k := y.KeyWithTs([]byte(key("key", i)), math.MaxUint64)
		keyHash := farm.Fingerprint64(k.UserKey)
		k1, _, ok, err := table.pointGet(k, keyHash)
		require.NoError(t, err)
		if !ok {
			// will fallback to seek
			continue
		}
		require.True(t, k1.SameUserKey(k), "point get not point to correct key")
	}

	for i := 8000; i < 10000; i++ {
		k := y.KeyWithTs([]byte(key("key", i)), math.MaxUint64)
		keyHash := farm.Fingerprint64(k.UserKey)
		rk, _, ok, err := table.pointGet(k, keyHash)
		require.NoError(t, err)
		if !ok {
			// will fallback to seek
			continue
		}
		if rk.IsEmpty() {
			// hash table says no entry, fast return
			continue
		}
		require.False(t, k.SameUserKey(rk), "point get not point to correct key")
	}
}

func TestExternalTable(t *testing.T) {
	filename := fmt.Sprintf("%s%s%x.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
	f, err := y.OpenSyncedFile(filename, true)
	if t != nil {
		require.NoError(t, err)
	} else {
		y.Check(err)
	}

	n := 200
	b := NewExternalTableBuilder(f, rate.NewLimiter(rate.Inf, math.MaxInt32), defaultBuilderOpt)
	kvs := generateKeyValues("key", n)
	for _, kv := range kvs {
		y.Assert(len(kv) == 2)
		err := b.Add(y.KeyWithTs([]byte(kv[0]), 0), y.ValueStruct{Value: []byte(kv[1]), Meta: 'A', UserMeta: []byte{0}})
		if t != nil {
			require.NoError(t, err)
		} else {
			y.Check(err)
		}
	}
	_, err = b.Finish()
	y.Check(err)
	f.Close()
	table, err := OpenTestTable(filename, testCache(), testCache())
	require.NoError(t, err)
	require.NoError(t, table.SetGlobalTs(10))

	require.NoError(t, table.Close())
	table, err = OpenTestTable(filename, testCache(), testCache())
	require.NoError(t, err)
	defer table.Delete()

	it := table.newIterator(false)
	defer it.Close()
	count := 0
	for it.Rewind(); it.Valid(); it.Next() {
		v := it.Value()
		k := y.KeyWithTs([]byte(key("key", count)), 10)
		require.EqualValues(t, k, it.Key())
		require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
		count++
	}
	require.Equal(t, count, n)
}

func TestSeekToFirst(t *testing.T) {
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTestTable(f.Name(), testCache(), testCache())
			require.NoError(t, err)
			defer table.Delete()
			it := table.newIterator(false)
			defer it.Close()
			it.seekToFirst()
			require.True(t, it.Valid())
			v := it.Value()
			require.EqualValues(t, "0", string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
		})
	}
}

func TestSeekToLast(t *testing.T) {
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTestTable(f.Name(), testCache(), testCache())
			require.NoError(t, err)
			defer table.Delete()
			it := table.newIterator(false)
			defer it.Close()
			it.seekToLast()
			require.True(t, it.Valid())
			v := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", n-1), string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
			it.prev()
			require.True(t, it.Valid())
			v = it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", n-2), string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
		})
	}
}

func TestSeekBasic(t *testing.T) {
	f := buildTestTable(t, "k", 10000)
	table, err := OpenTestTable(f.Name(), testCache(), testCache())
	require.NoError(t, err)
	defer table.Delete()

	it := table.newIterator(false)
	defer it.Close()

	var data = []struct {
		in    string
		valid bool
		out   string
	}{
		{"abc", true, "k0000"},
		{"k0100", true, "k0100"},
		{"k0100b", true, "k0101"}, // Test case where we jump to next block.
		{"k1234", true, "k1234"},
		{"k1234b", true, "k1235"},
		{"k9999", true, "k9999"},
		{"z", false, ""},
	}

	for _, tt := range data {
		it.seek([]byte(tt.in))
		if !tt.valid {
			require.False(t, it.Valid())
			continue
		}
		require.True(t, it.Valid())
		k := it.Key()
		require.EqualValues(t, tt.out, string(k.UserKey))
	}
}

func TestSeekForPrev(t *testing.T) {
	f := buildTestTable(t, "k", 10000)
	table, err := OpenTestTable(f.Name(), testCache(), testCache())
	require.NoError(t, err)
	defer table.Delete()

	it := table.newIterator(false)
	defer it.Close()

	var data = []struct {
		in    string
		valid bool
		out   string
	}{
		{"abc", false, ""},
		{"k0100", true, "k0100"},
		{"k0100b", true, "k0100"}, // Test case where we jump to next block.
		{"k1234", true, "k1234"},
		{"k1234b", true, "k1234"},
		{"k9999", true, "k9999"},
		{"z", true, "k9999"},
	}

	for _, tt := range data {
		it.seekForPrev([]byte(tt.in))
		if !tt.valid {
			require.False(t, it.Valid())
			continue
		}
		require.True(t, it.Valid())
		k := it.Key()
		require.EqualValues(t, tt.out, string(k.UserKey))
	}
}

func TestIterateFromStart(t *testing.T) {
	// Vary the number of elements added.
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTestTable(f.Name(), testCache(), testCache())
			require.NoError(t, err)
			defer table.Delete()
			ti := table.newIterator(false)
			defer ti.Close()
			ti.reset()
			ti.seekToFirst()
			require.True(t, ti.Valid())
			// No need to do a Next.
			// ti.Seek brings us to the first key >= "". Essentially a SeekToFirst.
			var count int
			for ; ti.Valid(); ti.next() {
				v := ti.Value()
				require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
				require.EqualValues(t, 'A', v.Meta)
				count++
			}
			require.EqualValues(t, n, count)
		})
	}
}

func TestIterateFromEnd(t *testing.T) {
	// Vary the number of elements added.
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTestTable(f.Name(), testCache(), testCache())
			require.NoError(t, err)
			defer table.Delete()
			ti := table.newIterator(false)
			defer ti.Close()
			ti.reset()
			ti.seek([]byte("zzzzzz")) // Seek to end, an invalid element.
			require.False(t, ti.Valid())
			ti.seekToLast()
			for i := n - 1; i >= 0; i-- {
				require.True(t, ti.Valid())
				v := ti.Value()
				require.EqualValues(t, fmt.Sprintf("%d", i), string(v.Value))
				require.EqualValues(t, 'A', v.Meta)
				ti.prev()
			}
			ti.prev()
			require.False(t, ti.Valid())
		})
	}
}

func TestTable(t *testing.T) {
	f := buildTestTable(t, "key", 10000)
	table, err := OpenTestTable(f.Name(), testCache(), testCache())
	require.NoError(t, err)
	defer table.Delete()
	ti := table.newIterator(false)
	defer ti.Close()
	kid := 1010
	seek := y.KeyWithTs([]byte(key("key", kid)), 0)
	for ti.seek(seek.UserKey); ti.Valid(); ti.next() {
		k := ti.Key()
		require.EqualValues(t, string(k.UserKey), key("key", kid))
		kid++
	}
	if kid != 10000 {
		t.Errorf("Expected kid: 10000. Got: %v", kid)
	}

	ti.seek([]byte(key("key", 99999)))
	require.False(t, ti.Valid())

	ti.seek([]byte(key("key", -1)))
	require.True(t, ti.Valid())
	k := ti.Key()
	require.EqualValues(t, string(k.UserKey), key("key", 0))
}

func TestIterateBackAndForth(t *testing.T) {
	f := buildTestTable(t, "key", 10000)
	table, err := OpenTestTable(f.Name(), testCache(), testCache())
	require.NoError(t, err)
	defer table.Delete()

	seek := y.KeyWithTs([]byte(key("key", 1010)), 0)
	it := table.newIterator(false)
	defer it.Close()
	it.seek(seek.UserKey)
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, seek, k)

	it.prev()
	it.prev()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 1008), string(k.UserKey))

	it.next()
	it.next()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 1010), k.UserKey)

	it.seek([]byte(key("key", 2000)))
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 2000), k.UserKey)

	it.prev()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 1999), k.UserKey)

	it.seekToFirst()
	k = it.Key()
	require.EqualValues(t, key("key", 0), k.UserKey)
}

func TestIterateMultiVersion(t *testing.T) {
	f, allCnt := buildMultiVersionTable(generateKeyValues("key", 4000))
	table, err := OpenTestTable(f.Name(), testCache(), testCache())
	require.NoError(t, err)
	defer table.Delete()
	it := table.newIterator(false)
	defer it.Close()
	itCnt := 0
	var lastKey y.Key
	for it.Rewind(); it.Valid(); it.Next() {
		if !lastKey.IsEmpty() {
			require.True(t, lastKey.Compare(it.Key()) < 0)
		}
		lastKey.Copy(it.Key())
		itCnt++
		for it.NextVersion() {
			itCnt++
		}
	}
	require.Equal(t, itCnt, allCnt)
	for i := 0; i < 1000; i++ {
		k := y.KeyWithTs([]byte(key("key", int(z.FastRand()%4000))), uint64(5+z.FastRand()%5))
		kHash := farm.Fingerprint64(k.UserKey)
		gotKey, _, ok, _ := table.pointGet(k, kHash)
		if ok {
			if !gotKey.IsEmpty() {
				require.True(t, gotKey.SameUserKey(k))
				require.True(t, gotKey.Compare(k) >= 0)
			}
		} else {
			it.Seek(k.UserKey)
			if it.Valid() {
				require.True(t, it.Key().Version == 9)
				require.True(t, bytes.Compare(it.Key().UserKey, k.UserKey) >= 0)
				if y.SeekToVersion(it, k.Version) {
					require.True(t, it.Key().Version <= k.Version)
				}
			}
		}
	}
	revIt := table.newIterator(true)
	defer revIt.Close()
	lastKey.Reset()
	for revIt.Rewind(); revIt.Valid(); revIt.Next() {
		if !lastKey.IsEmpty() {
			require.Truef(t, lastKey.Compare(revIt.Key()) > 0, "%v %v", lastKey.String(), revIt.Key().String())
		}
		lastKey.Copy(revIt.Key())
	}
	for i := 0; i < 1000; i++ {
		k := y.KeyWithTs([]byte(key("key", int(z.FastRand()%4000))), uint64(5+z.FastRand()%5))
		// reverse iterator never seek to the same key with smaller version.
		revIt.Seek(k.UserKey)
		if !revIt.Valid() {
			continue
		}
		require.True(t, revIt.Key().Version == 9)
		require.True(t, revIt.Key().Compare(k) <= 0, "%s %s", revIt.Key(), k)
	}
}

func TestUniIterator(t *testing.T) {
	f := buildTestTable(t, "key", 10000)
	table, err := OpenTestTable(f.Name(), testCache(), testCache())
	require.NoError(t, err)
	defer table.Delete()
	{
		it := table.newIterator(false)
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			v := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
			count++
		}
		require.EqualValues(t, 10000, count)
		it.Close()
	}
	{
		it := table.newIterator(true)
		var count int
		for it.Rewind(); it.Valid(); it.Next() {
			v := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", 10000-1-count), string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
			count++
		}
		require.EqualValues(t, 10000, count)
		it.Close()
	}
}

// Try having only one table.
func TestConcatIteratorOneTable(t *testing.T) {
	f := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})

	tbl, err := OpenTestTable(f.Name(), testCache(), testCache())
	require.NoError(t, err)
	defer tbl.Delete()

	it := table.NewConcatIterator([]table.Table{tbl}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(k.UserKey))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
}

func TestConcatIterator(t *testing.T) {
	f := buildTestTable(t, "keya", 10000)
	f2 := buildTestTable(t, "keyb", 10000)
	f3 := buildTestTable(t, "keyc", 10000)
	blkCache, idxCache := testCache(), testCache()
	tbl, err := OpenTestTable(f.Name(), blkCache, idxCache)
	require.NoError(t, err)
	defer tbl.Delete()
	tbl2, err := OpenTestTable(f2.Name(), blkCache, idxCache)
	require.NoError(t, err)
	defer tbl2.Delete()
	tbl3, err := OpenTestTable(f3.Name(), blkCache, idxCache)
	require.NoError(t, err)
	defer tbl3.Delete()

	{
		it := table.NewConcatIterator([]table.Table{tbl, tbl2, tbl3}, false)
		it.Rewind()
		require.True(t, it.Valid())
		var count int
		for ; it.Valid(); it.Next() {
			vs := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", count%10000), string(vs.Value))
			require.EqualValues(t, 'A', vs.Meta)
			count++
		}
		require.EqualValues(t, 30000, count)

		it.Seek([]byte("a"))
		require.EqualValues(t, "keya0000", string(it.Key().UserKey))
		vs := it.Value()
		require.EqualValues(t, "0", string(vs.Value))

		it.Seek([]byte("keyb"))
		require.EqualValues(t, "keyb0000", string(it.Key().UserKey))
		vs = it.Value()
		require.EqualValues(t, "0", string(vs.Value))

		it.Seek([]byte("keyb9999b"))
		require.EqualValues(t, "keyc0000", string(it.Key().UserKey))
		vs = it.Value()
		require.EqualValues(t, "0", string(vs.Value))

		it.Seek([]byte("keyd"))
		require.False(t, it.Valid())
		it.Close()
	}
	{
		it := table.NewConcatIterator([]table.Table{tbl, tbl2, tbl3}, true)
		it.Rewind()
		require.True(t, it.Valid())
		var count int
		for ; it.Valid(); it.Next() {
			vs := it.Value()
			require.EqualValues(t, fmt.Sprintf("%d", 10000-(count%10000)-1), string(vs.Value))
			require.EqualValues(t, 'A', vs.Meta)
			count++
		}
		require.EqualValues(t, 30000, count)

		it.Seek([]byte("a"))
		require.False(t, it.Valid())

		it.Seek([]byte("keyb"))
		require.EqualValues(t, "keya9999", string(it.Key().UserKey))
		vs := it.Value()
		require.EqualValues(t, "9999", string(vs.Value))

		it.Seek([]byte("keyb9999b"))
		require.EqualValues(t, "keyb9999", string(it.Key().UserKey))
		vs = it.Value()
		require.EqualValues(t, "9999", string(vs.Value))

		it.Seek([]byte("keyd"))
		require.EqualValues(t, "keyc9999", string(it.Key().UserKey))
		vs = it.Value()
		require.EqualValues(t, "9999", string(vs.Value))
		it.Close()
	}
}

func TestMergingIterator(t *testing.T) {
	f1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})
	f2 := buildTable(t, [][]string{
		{"k1", "b1"},
		{"k2", "b2"},
	})
	blkCache, idxCache := testCache(), testCache()
	tbl1, err := OpenTestTable(f1.Name(), blkCache, idxCache)
	require.NoError(t, err)
	defer tbl1.Delete()
	tbl2, err := OpenTestTable(f2.Name(), blkCache, idxCache)
	require.NoError(t, err)
	defer tbl2.Delete()
	it1 := tbl1.newIterator(false)
	it2 := table.NewConcatIterator([]table.Table{tbl2}, false)
	it := table.NewMergeIterator([]y.Iterator{it1, it2}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(k.UserKey))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(k.UserKey))
	vs = it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.False(t, it.Valid())
}

func TestMergingIteratorReversed(t *testing.T) {
	f1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})
	f2 := buildTable(t, [][]string{
		{"k1", "b1"},
		{"k2", "b2"},
	})
	blkCache, idxCache := testCache(), testCache()
	tbl1, err := OpenTestTable(f1.Name(), blkCache, idxCache)
	require.NoError(t, err)
	defer tbl1.Delete()
	tbl2, err := OpenTestTable(f2.Name(), blkCache, idxCache)
	require.NoError(t, err)
	defer tbl2.Delete()
	it1 := tbl1.newIterator(true)
	it2 := table.NewConcatIterator([]table.Table{tbl2}, true)
	it := table.NewMergeIterator([]y.Iterator{it1, it2}, true)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k2", string(k.UserKey))
	vs := it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k1", string(k.UserKey))
	vs = it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.False(t, it.Valid())
}

// Take only the first iterator.
func TestMergingIteratorTakeOne(t *testing.T) {
	f1 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})
	f2 := buildTable(t, [][]string{{"l1", "b1"}})

	blkCache, idxCache := testCache(), testCache()
	t1, err := OpenTestTable(f1.Name(), blkCache, idxCache)
	require.NoError(t, err)
	defer t1.Delete()
	t2, err := OpenTestTable(f2.Name(), blkCache, idxCache)
	require.NoError(t, err)
	defer t2.Delete()

	it1 := table.NewConcatIterator([]table.Table{t1}, false)
	it2 := table.NewConcatIterator([]table.Table{t2}, false)
	it := table.NewMergeIterator([]y.Iterator{it1, it2}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(k.UserKey))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(k.UserKey))
	vs = it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	k = it.Key()
	require.EqualValues(t, "l1", string(k.UserKey))
	vs = it.Value()
	require.EqualValues(t, "b1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.False(t, it.Valid())
}

// Take only the second iterator.
func TestMergingIteratorTakeTwo(t *testing.T) {
	f1 := buildTable(t, [][]string{{"l1", "b1"}})
	f2 := buildTable(t, [][]string{
		{"k1", "a1"},
		{"k2", "a2"},
	})
	blkCache, idxCache := testCache(), testCache()
	t1, err := OpenTestTable(f1.Name(), blkCache, idxCache)
	require.NoError(t, err)
	defer t1.Delete()
	t2, err := OpenTestTable(f2.Name(), blkCache, idxCache)
	require.NoError(t, err)
	defer t2.Delete()

	it1 := table.NewConcatIterator([]table.Table{t1}, false)
	it2 := table.NewConcatIterator([]table.Table{t2}, false)
	it := table.NewMergeIterator([]y.Iterator{it1, it2}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(k.UserKey))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(k.UserKey))
	vs = it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()
	require.True(t, it.Valid())

	k = it.Key()
	require.EqualValues(t, "l1", string(k.UserKey))
	vs = it.Value()
	require.EqualValues(t, "b1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.False(t, it.Valid())
}

func TestSeekInvalidIssue(t *testing.T) {
	keys := make([][]byte, 1024)
	for i := 0; i < len(keys); i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		keys[i] = key
	}
	// TODO:
	// FIXME: if set surf to true it always fail.
	b, f := newTableBuilderForTest(false)
	for _, key := range keys {
		err := b.Add(y.KeyWithTs(key, 0), y.ValueStruct{Value: key, Meta: 'A', UserMeta: []byte{0}})
		if t != nil {
			require.NoError(t, err)
		} else {
			y.Check(err)
		}
	}
	_, err := b.Finish()
	y.Check(err)
	y.Check(f.Close())
	f, _ = y.OpenSyncedFile(f.Name(), true)
	t1, err := OpenTestTable(f.Name(), nil, nil)

	require.NoError(t, err)
	defer t1.Delete()
	it := t1.NewIterator(false).(*Iterator)
	defer it.Close()
	for i := 1; i < it.tIdx.baseKeys.length(); i++ {
		baseKey := it.tIdx.baseKeys.getEntry(i)
		idx := sort.Search(len(keys), func(i int) bool {
			return bytes.Compare(keys[i], baseKey) >= 0
		})
		seekKey := y.SafeCopy(nil, keys[idx-1])
		seekKey = append(seekKey, 0)
		it.Seek(seekKey)
		require.True(t, it.Valid(), "candidate key %v, baseKey %v %d", seekKey, baseKey, i)
	}
}

func TestOpenImMemoryTable(t *testing.T) {
	file := buildTestTable(t, "in-mem", 1000)
	blockData, err := ioutil.ReadFile(file.Name())
	require.Nil(t, err)
	idxData, err := ioutil.ReadFile(IndexFilename(file.Name()))
	require.Nil(t, err)
	inMemTbl, err := OpenInMemoryTable(NewInMemFile(blockData, idxData))
	require.Nil(t, err)
	fileTable, err := OpenTestTable(file.Name(), nil, nil)
	require.Nil(t, err)
	inMemIt := inMemTbl.NewIterator(false)
	defer inMemIt.Close()
	inMemIt.Rewind()
	fileIt := fileTable.NewIterator(false)
	defer fileIt.Close()
	fileIt.Rewind()
	for ; fileIt.Valid(); fileIt.Next() {
		if fileIt.Valid() {
			require.True(t, inMemIt.Valid())
			require.EqualValues(t, fileIt.Key(), inMemIt.Key())
			require.EqualValues(t, fileIt.Value(), inMemIt.Value())
		}
		inMemIt.Next()
	}
}

func TestBuildImMemoryTable(t *testing.T) {
	b := NewTableBuilder(nil, nil, 0, defaultBuilderOpt)
	keyValues := generateKeyValues("in-mem", 1000)
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for _, kv := range keyValues {
		y.Assert(len(kv) == 2)
		err := b.Add(y.KeyWithTs([]byte(kv[0]), 0), y.ValueStruct{Value: []byte(kv[1]), Meta: 'A', UserMeta: []byte{0}})
		if t != nil {
			require.NoError(t, err)
		} else {
			y.Check(err)
		}
	}
	result, err := b.Finish()
	y.Check(err)
	require.NotNil(t, result.FileData)
	require.NotNil(t, result.IndexData)
	tbl, err := OpenInMemoryTable(NewInMemFile(result.FileData, result.IndexData))
	y.Check(err)
	for _, kv := range keyValues {
		key := y.KeyWithTs([]byte(kv[0]), 0)
		keyHash := farm.Fingerprint64(key.UserKey)
		v, err := tbl.Get(key, keyHash)
		require.Nil(t, err)
		require.EqualValues(t, v.Value, []byte(kv[1]))
	}
}

func BenchmarkRead(b *testing.B) {
	n := 5 << 20
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
	f, err := y.OpenSyncedFile(filename, true)
	y.Check(err)
	builder := NewTableBuilder(f, nil, 0, defaultBuilderOpt)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%016x", i)
		v := fmt.Sprintf("%d", i)
		y.Check(builder.Add(y.KeyWithTs([]byte(k), 0), y.ValueStruct{Value: []byte(v), Meta: 123, UserMeta: []byte{0}}))
	}
	_, err = builder.Finish()
	y.Check(err)
	tbl, err := OpenTestTable(f.Name(), testCache(), testCache())
	y.Check(err)
	defer tbl.Delete()

	//	y.Printf("Size of table: %d\n", tbl.Size())
	b.ResetTimer()
	// Iterate b.N times over the entire table.
	for i := 0; i < b.N; i++ {
		func() {
			it := tbl.newIterator(false)
			for it.seekToFirst(); it.Valid(); it.next() {
			}
			it.Close()
		}()
	}
}

func BenchmarkBuildTable(b *testing.B) {
	ns := []int{1000, 10000, 100000, 1000000, 5000000, 10000000, 15000000}
	for _, n := range ns {
		kvs := make([]struct {
			k y.Key
			v []byte
		}, n)
		for i := 0; i < n; i++ {
			kvs[i].k = y.KeyWithTs([]byte(fmt.Sprintf("%016x", i)), 0)
			kvs[i].v = []byte(fmt.Sprintf("%d", i))
		}
		b.ResetTimer()

		b.Run(fmt.Sprintf("NoHash_%d", n), func(b *testing.B) {
			filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
			f, err := y.OpenSyncedFile(filename, false)
			y.Check(err)
			opt := defaultBuilderOpt
			for bn := 0; bn < b.N; bn++ {
				builder := NewTableBuilder(f, nil, 0, opt)
				for i := 0; i < n; i++ {
					y.Check(builder.Add(kvs[i].k, y.ValueStruct{Value: kvs[i].v, Meta: 123, UserMeta: []byte{0}}))
				}
				_, err = builder.Finish()
				y.Check(err)
				_, err := f.Seek(0, io.SeekStart)
				y.Check(err)
			}
		})

		b.Run(fmt.Sprintf("Hash_%d", n), func(b *testing.B) {
			filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
			f, err := y.OpenSyncedFile(filename, false)
			y.Check(err)
			for bn := 0; bn < b.N; bn++ {
				builder := NewTableBuilder(f, nil, 0, defaultBuilderOpt)
				for i := 0; i < n; i++ {
					y.Check(builder.Add(kvs[i].k, y.ValueStruct{Value: kvs[i].v, Meta: 123, UserMeta: []byte{0}}))
				}
				_, err = builder.Finish()
				y.Check(err)
				_, err := f.Seek(0, io.SeekStart)
				y.Check(err)
			}
		})
	}
}

var cacheConfig = cache.Config{
	NumCounters: 1000000 * 10,
	MaxCost:     1000000,
	BufferItems: 64,
	Metrics:     true,
}

func BenchmarkPointGet(b *testing.B) {
	ns := []int{1000, 10000, 100000, 1000000, 5000000, 10000000, 15000000}
	for _, n := range ns {
		filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
		f, err := y.OpenSyncedFile(filename, true)
		builder := NewTableBuilder(f, nil, 0, defaultBuilderOpt)
		keys := make([]y.Key, n)
		y.Check(err)
		for i := 0; i < n; i++ {
			k := y.KeyWithTs([]byte(fmt.Sprintf("%016x", i)), 0)
			v := fmt.Sprintf("%d", i)
			keys[i] = k
			y.Check(builder.Add(k, y.ValueStruct{Value: []byte(v), Meta: 123, UserMeta: []byte{0}}))
		}
		_, err = builder.Finish()
		y.Check(err)
		tbl, err := OpenTestTable(filename, testCache(), testCache())
		y.Check(err)
		b.ResetTimer()

		b.Run(fmt.Sprintf("Seek_%d", n), func(b *testing.B) {
			var vs y.ValueStruct
			rand := rand.New(rand.NewSource(0))
			for bn := 0; bn < b.N; bn++ {
				rand.Seed(0)
				for i := 0; i < n; i++ {
					k := keys[rand.Intn(n)]
					it := tbl.newIterator(false)
					defer it.Close()
					it.Seek(k.UserKey)
					if !it.Valid() {
						continue
					}
					if !k.SameUserKey(it.Key()) {
						continue
					}
					vs = it.Value()
				}
			}
			_ = vs
		})

		b.Run(fmt.Sprintf("Hash_%d", n), func(b *testing.B) {
			var (
				resultKey y.Key
				resultVs  y.ValueStruct
				ok        bool
			)
			rand := rand.New(rand.NewSource(0))
			for bn := 0; bn < b.N; bn++ {
				rand.Seed(0)
				for i := 0; i < n; i++ {
					k := keys[rand.Intn(n)]
					keyHash := farm.Fingerprint64(k.UserKey)
					resultKey, resultVs, ok, _ = tbl.pointGet(k, keyHash)
					if !ok {
						it := tbl.newIterator(false)
						defer it.Close()
						it.Seek(k.UserKey)
						if !it.Valid() {
							continue
						}
						if !k.SameUserKey(it.Key()) {
							continue
						}
						resultKey, resultVs = it.Key(), it.Value()
					}
				}
			}
			_, _ = resultKey, resultVs
		})

		tbl.Delete()
	}
}

func BenchmarkReadAndBuild(b *testing.B) {
	n := 5 << 20
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
	f, err := y.OpenSyncedFile(filename, false)
	builder := NewTableBuilder(f, nil, 0, defaultBuilderOpt)
	y.Check(err)
	for i := 0; i < n; i++ {
		k := y.KeyWithTs([]byte(fmt.Sprintf("%016x", i)), 0)
		v := fmt.Sprintf("%d", i)
		y.Check(builder.Add(k, y.ValueStruct{Value: []byte(v), Meta: 123, UserMeta: []byte{0}}))
	}
	_, err = builder.Finish()
	y.Check(err)
	tbl, err := OpenTestTable(f.Name(), testCache(), testCache())
	y.Check(err)
	defer tbl.Delete()

	//	y.Printf("Size of table: %d\n", tbl.Size())
	b.ResetTimer()
	// Iterate b.N times over the entire table.
	filename = fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
	f, err = y.OpenSyncedFile(filename, false)
	y.Check(err)
	for i := 0; i < b.N; i++ {
		func() {
			newBuilder := NewTableBuilder(f, nil, 0, options.TableBuilderOptions{})
			it := tbl.newIterator(false)
			defer it.Close()
			for it.seekToFirst(); it.Valid(); it.next() {
				vs := it.Value()
				newBuilder.Add(it.Key(), vs)
			}
			_, err = newBuilder.Finish()
			y.Check(err)
			_, err := f.Seek(0, io.SeekStart)
			y.Check(err)
		}()
	}
}

func BenchmarkReadMerged(b *testing.B) {
	n := 5 << 20
	m := 5 // Number of tables.
	y.Assert((n % m) == 0)
	tableSize := n / m
	var tables []*Table

	for i := 0; i < m; i++ {
		filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
		f, err := y.OpenSyncedFile(filename, true)
		y.Check(err)
		builder := NewTableBuilder(f, nil, 0, defaultBuilderOpt)
		for j := 0; j < tableSize; j++ {
			id := j*m + i // Arrays are interleaved.
			// id := i*tableSize+j (not interleaved)
			k := y.KeyWithTs([]byte(fmt.Sprintf("%016x", id)), 0)
			v := fmt.Sprintf("%d", id)
			y.Check(builder.Add(k, y.ValueStruct{Value: []byte(v), Meta: 123, UserMeta: []byte{0}}))
		}
		_, err = builder.Finish()
		y.Check(err)
		tbl, err := OpenTestTable(f.Name(), testCache(), testCache())
		y.Check(err)
		tables = append(tables, tbl)
		defer tbl.Delete()
	}

	b.ResetTimer()
	// Iterate b.N times over the entire table.
	for i := 0; i < b.N; i++ {
		func() {
			var iters []y.Iterator
			for _, tbl := range tables {
				iters = append(iters, tbl.newIterator(false))
			}
			it := table.NewMergeIterator(iters, false)
			for it.Rewind(); it.Valid(); it.Next() {
			}
			it.Close()
		}()
	}
}

func BenchmarkRandomRead(b *testing.B) {
	n := int(5 * 1e6)
	tbl := getTableForBenchmarks(b, n, testCache(), testCache())

	r := rand.New(rand.NewSource(time.Now().Unix()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		itr := tbl.newIterator(false)
		no := r.Intn(n)
		k := []byte(fmt.Sprintf("%016x", no))
		v := []byte(fmt.Sprintf("%d", no))
		itr.Seek(k)
		if !itr.Valid() {
			b.Fatal("itr should be valid")
		}
		v1 := itr.Value().Value

		if !bytes.Equal(v, v1) {
			fmt.Println("value does not match")
			b.Fatal()
		}
		itr.Close()
	}
}

func getTableForBenchmarks(b *testing.B, count int, blkCache, idxCache *cache.Cache) *Table {
	rand.Seed(time.Now().Unix())
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
	f, err := y.OpenSyncedFile(filename, false)
	builder := NewTableBuilder(f, nil, 0, defaultBuilderOpt)
	require.NoError(b, err)
	for i := 0; i < count; i++ {
		k := y.KeyWithTs([]byte(fmt.Sprintf("%016x", i)), 0)
		v := fmt.Sprintf("%d", i)
		builder.Add(k, y.ValueStruct{Value: []byte(v)})
	}

	_, err = builder.Finish()
	require.NoError(b, err, "unable to write to file")
	tbl, err := OpenTestTable(f.Name(), blkCache, idxCache)
	require.NoError(b, err, "unable to open table")
	return tbl
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UTC().UnixNano())
	os.Exit(m.Run())
}

func OpenTestTable(filename string, blockCache, idxCache *cache.Cache) (*Table, error) {
	var tf TableFile
	var err error
	if blockCache != nil {
		tf, err = NewLocalFile(filename, blockCache, idxCache)
	} else {
		tf, err = NewMMapFile(filename)
	}
	if err != nil {
		return nil, err
	}
	return OpenTable(filename, tf)
}
