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
	"github.com/ngaut/unistore/sdb/cache"
	"github.com/ngaut/unistore/sdb/cache/z"
	"github.com/ngaut/unistore/sdb/table"
	"github.com/pingcap/badger/y"
	"github.com/stretchr/testify/require"
)

func key(prefix string, i int) string {
	return prefix + fmt.Sprintf("%04d", i)
}

var defaultBuilderOpt = TableBuilderOptions{
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

func OpenTestTable(filename string, cache *cache.Cache) (*Table, error) {
	tf, err := NewLocalFile(filename, cache == nil)
	if err != nil {
		return nil, err
	}
	return OpenTable(tf, cache)
}

func buildTestTable(t *testing.T, prefix string, n int) *os.File {
	return buildTable(t, generateKeyValues(prefix, n))
}

// keyValues is n by 2 where n is number of pairs.
func buildTable(t *testing.T, keyValues [][]string) *os.File {
	// TODO: Add test for file garbage collection here. No files should be left after the tests here.
	b, f := newTableBuilderForTest(t)
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for _, kv := range keyValues {
		y.Assert(len(kv) == 2)
		b.Add([]byte(kv[0]), &y.ValueStruct{Value: []byte(kv[1]), Meta: 'A', UserMeta: []byte{0}})
	}
	_, err := b.Finish(f.Name(), f)
	y.Check(err)
	y.Check(f.Close())
	f, _ = y.OpenSyncedFile(f.Name(), true)
	return f
}

func newTableBuilderForTest(t *testing.T) (*Builder, *os.File) {
	filename := fmt.Sprintf("%s%s%x.sst", os.TempDir(), string(os.PathSeparator), z.FastRand())
	f, err := y.OpenSyncedFile(filename, true)
	y.Check(err)
	opt := defaultBuilderOpt
	y.Assert(filename == f.Name())
	id, ok := ParseFileID(filename)
	require.True(t, ok)
	return NewTableBuilder(id, opt), f
}

func buildMultiVersionTable(t *testing.T, keyValues [][]string) (*os.File, int) {
	b, f := newTableBuilderForTest(t)
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	allCnt := len(keyValues)
	for _, kv := range keyValues {
		y.Assert(len(kv) == 2)
		val := fmt.Sprintf("%s_%d", kv[1], 9)
		b.Add([]byte(kv[0]), &y.ValueStruct{Value: []byte(val), Meta: 'A', UserMeta: []byte{0}, Version: 9})
		for i := uint64(8); i > 0; i-- {
			if z.FastRand()%4 == 0 {
				val = fmt.Sprintf("%s_%d", kv[1], i)
				b.Add([]byte(kv[0]), &y.ValueStruct{Value: []byte(val), Meta: 'A', UserMeta: []byte{0}, Version: i})
				allCnt++
			}
		}
	}
	_, err := b.Finish(f.Name(), f)
	y.Check(err)
	y.Check(f.Close())
	f, _ = y.OpenSyncedFile(f.Name(), true)
	return f, allCnt
}

func TestTableIterator(t *testing.T) {
	for _, n := range []int{99, 100, 101} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTestTable(f.Name(), nil)
			require.NoError(t, err)
			defer table.Delete()
			it := table.NewIterator(false)
			count := 0
			for it.Rewind(); it.Valid(); it.Next() {
				v := it.Value()
				k := []byte(key("key", count))
				require.EqualValues(t, k, it.Key())
				require.EqualValues(t, fmt.Sprintf("%d", count), string(v.Value))
				count++
			}
			require.Equal(t, count, n)
			it.Close()
		})
	}
}

func TestPointGet(t *testing.T) {
	keyVals := generateKeyValues("key", 8000)
	f := buildTable(t, keyVals)
	table, err := OpenTestTable(f.Name(), nil)
	require.NoError(t, err)
	defer table.Delete()

	for i := 0; i < 8000; i++ {
		k := []byte(key("key", i))
		keyHash := farm.Fingerprint64(k)
		val, err1 := table.Get(k, math.MaxUint64, keyHash)
		require.NoError(t, err1)
		require.True(t, string(val.Value) == keyVals[i][1], "point get not point to correct key %d %s", i, val.Value)
	}

	for i := 8000; i < 10000; i++ {
		k := []byte(key("key", i))
		keyHash := farm.Fingerprint64(k)
		val, err1 := table.Get(k, math.MaxUint64, keyHash)
		require.NoError(t, err1)
		require.True(t, !val.Valid())
	}
}

func TestSeekToFirst(t *testing.T) {
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTestTable(f.Name(), nil)
			require.NoError(t, err)
			defer table.Delete()
			it := table.NewIterator(false).(*Iterator)
			defer it.Close()
			it.seekToFirst()
			require.True(t, it.Valid())
			v := it.Value()
			require.EqualValues(t, "0", string(v.Value))
			require.EqualValues(t, 'A', v.Meta)
			require.EqualValues(t, []byte{0}, v.UserMeta)
		})
	}
}

func TestSeekToLast(t *testing.T) {
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTestTable(f.Name(), nil)
			require.NoError(t, err)
			defer table.Delete()
			it := table.NewIterator(false).(*Iterator)
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
	table, err := OpenTestTable(f.Name(), nil)
	require.NoError(t, err)
	defer table.Delete()

	it := table.NewIterator(false)
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
		it.Seek([]byte(tt.in))
		if !tt.valid {
			require.False(t, it.Valid())
			continue
		}
		require.True(t, it.Valid())
		k := it.Key()
		require.EqualValues(t, tt.out, string(k))
	}
}

func TestSeekForPrev(t *testing.T) {
	f := buildTestTable(t, "k", 10000)
	table, err := OpenTestTable(f.Name(), nil)
	require.NoError(t, err)
	defer table.Delete()

	it := table.NewIterator(true)
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
		it.Seek([]byte(tt.in))
		if !tt.valid {
			require.False(t, it.Valid())
			continue
		}
		require.True(t, it.Valid())
		k := it.Key()
		require.EqualValues(t, tt.out, string(k))
	}
}

func TestIterateFromStart(t *testing.T) {
	// Vary the number of elements added.
	for _, n := range []int{99, 100, 101, 199, 200, 250, 9999, 10000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			f := buildTestTable(t, "key", n)
			table, err := OpenTestTable(f.Name(), nil)
			require.NoError(t, err)
			defer table.Delete()
			ti := table.NewIterator(false)
			defer ti.Close()
			ti.Rewind()
			require.True(t, ti.Valid())
			// No need to do a Next.
			// ti.Seek brings us to the first key >= "". Essentially a SeekToFirst.
			var count int
			for ; ti.Valid(); ti.Next() {
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
			table, err := OpenTestTable(f.Name(), nil)
			require.NoError(t, err)
			defer table.Delete()
			ti := table.NewIterator(false).(*Iterator)
			defer ti.Close()
			ti.Seek([]byte("zzzzzz")) // Seek to end, an invalid element.
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
	table, err := OpenTestTable(f.Name(), nil)
	require.NoError(t, err)
	defer table.Delete()
	ti := table.NewIterator(false)
	defer ti.Close()
	kid := 1010
	seek := []byte(key("key", kid))
	for ti.Seek(seek); ti.Valid(); ti.Next() {
		k := ti.Key()
		require.EqualValues(t, string(k), key("key", kid))
		kid++
	}
	if kid != 10000 {
		t.Errorf("Expected kid: 10000. Got: %v", kid)
	}

	ti.Seek([]byte(key("key", 99999)))
	require.False(t, ti.Valid())

	ti.Seek([]byte(key("key", -1)))
	require.True(t, ti.Valid())
	k := ti.Key()
	require.EqualValues(t, string(k), key("key", 0))
}

func TestIterateBackAndForth(t *testing.T) {
	f := buildTestTable(t, "key", 10000)
	table, err := OpenTestTable(f.Name(), nil)
	require.NoError(t, err)
	defer table.Delete()

	seek := []byte(key("key", 1010))
	it := table.NewIterator(false).(*Iterator)
	defer it.Close()
	it.Seek(seek)
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, seek, k)

	it.prev()
	it.prev()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 1008), string(k))

	it.next()
	it.next()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 1010), k)

	it.seek([]byte(key("key", 2000)))
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 2000), k)

	it.prev()
	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, key("key", 1999), k)

	it.seekToFirst()
	k = it.Key()
	require.EqualValues(t, key("key", 0), k)
}

func TestIterateMultiVersion(t *testing.T) {
	f, allCnt := buildMultiVersionTable(t, generateKeyValues("key", 4000))
	table, err := OpenTestTable(f.Name(), nil)
	require.NoError(t, err)
	defer table.Delete()
	it := table.NewIterator(false)
	defer it.Close()
	itCnt := 0
	var lastKey []byte
	for it.Rewind(); it.Valid(); it.Next() {
		if len(lastKey) > 0 {
			require.True(t, bytes.Compare(lastKey, it.Key()) < 0)
		}
		lastKey = y.Copy(it.Key())
		itCnt++
		for it.NextVersion() {
			itCnt++
		}
	}
	require.Equal(t, itCnt, allCnt)
	for i := 0; i < 1000; i++ {
		k := []byte(key("key", int(z.FastRand()%4000)))
		ver := uint64(5 + z.FastRand()%5)
		kHash := farm.Fingerprint64(k)
		val, err1 := table.Get(k, ver, kHash)
		require.Nil(t, err1)
		if val.Valid() {
			require.True(t, val.Version <= ver)
		}
	}
	revIt := table.NewIterator(true)
	defer revIt.Close()
	lastKey = nil
	for revIt.Rewind(); revIt.Valid(); revIt.Next() {
		if len(lastKey) > 0 {
			require.Truef(t, bytes.Compare(lastKey, revIt.Key()) > 0, "%s %s", lastKey, revIt.Key())
		}
		lastKey = y.Copy(revIt.Key())
	}
	for i := 0; i < 1000; i++ {
		k := []byte(key("key", int(z.FastRand()%4000)))
		// reverse iterator never seek to the same key with smaller version.
		revIt.Seek(k)
		if !revIt.Valid() {
			continue
		}
		require.True(t, revIt.Value().Version == 9)
		require.True(t, bytes.Compare(revIt.Key(), k) <= 0, "%s %s", revIt.Key(), k)
	}
}

func TestUniIterator(t *testing.T) {
	f := buildTestTable(t, "key", 10000)
	table, err := OpenTestTable(f.Name(), nil)
	require.NoError(t, err)
	defer table.Delete()
	{
		it := table.NewIterator(false)
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
		it := table.NewIterator(true)
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

	tbl, err := OpenTestTable(f.Name(), nil)
	require.NoError(t, err)
	defer tbl.Delete()

	it := table.NewConcatIterator([]table.Table{tbl}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(k))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
}

func TestConcatIterator(t *testing.T) {
	f := buildTestTable(t, "keya", 10000)
	f2 := buildTestTable(t, "keyb", 10000)
	f3 := buildTestTable(t, "keyc", 10000)
	tbl, err := OpenTestTable(f.Name(), nil)
	require.NoError(t, err)
	defer tbl.Delete()
	tbl2, err := OpenTestTable(f2.Name(), nil)
	require.NoError(t, err)
	defer tbl2.Delete()
	tbl3, err := OpenTestTable(f3.Name(), nil)
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
		require.EqualValues(t, "keya0000", string(it.Key()))
		vs := it.Value()
		require.EqualValues(t, "0", string(vs.Value))

		it.Seek([]byte("keyb"))
		require.EqualValues(t, "keyb0000", string(it.Key()))
		vs = it.Value()
		require.EqualValues(t, "0", string(vs.Value))

		it.Seek([]byte("keyb9999b"))
		require.EqualValues(t, "keyc0000", string(it.Key()))
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
		require.EqualValues(t, "keya9999", string(it.Key()))
		vs := it.Value()
		require.EqualValues(t, "9999", string(vs.Value))

		it.Seek([]byte("keyb9999b"))
		require.EqualValues(t, "keyb9999", string(it.Key()))
		vs = it.Value()
		require.EqualValues(t, "9999", string(vs.Value))

		it.Seek([]byte("keyd"))
		require.EqualValues(t, "keyc9999", string(it.Key()))
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
	tbl1, err := OpenTestTable(f1.Name(), nil)
	require.NoError(t, err)
	defer tbl1.Delete()
	tbl2, err := OpenTestTable(f2.Name(), nil)
	require.NoError(t, err)
	defer tbl2.Delete()
	it1 := tbl1.NewIterator(false)
	it2 := table.NewConcatIterator([]table.Table{tbl2}, false)
	it := table.NewMergeIterator([]table.Iterator{it1, it2}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(k))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(k))
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
	tbl1, err := OpenTestTable(f1.Name(), nil)
	require.NoError(t, err)
	defer tbl1.Delete()
	tbl2, err := OpenTestTable(f2.Name(), nil)
	require.NoError(t, err)
	defer tbl2.Delete()
	it1 := tbl1.NewIterator(true)
	it2 := table.NewConcatIterator([]table.Table{tbl2}, true)
	it := table.NewMergeIterator([]table.Iterator{it1, it2}, true)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k2", string(k))
	vs := it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k1", string(k))
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

	t1, err := OpenTestTable(f1.Name(), nil)
	require.NoError(t, err)
	defer t1.Delete()
	t2, err := OpenTestTable(f2.Name(), nil)
	require.NoError(t, err)
	defer t2.Delete()

	it1 := table.NewConcatIterator([]table.Table{t1}, false)
	it2 := table.NewConcatIterator([]table.Table{t2}, false)
	it := table.NewMergeIterator([]table.Iterator{it1, it2}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(k))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(k))
	vs = it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	k = it.Key()
	require.EqualValues(t, "l1", string(k))
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
	t1, err := OpenTestTable(f1.Name(), nil)
	require.NoError(t, err)
	defer t1.Delete()
	t2, err := OpenTestTable(f2.Name(), nil)
	require.NoError(t, err)
	defer t2.Delete()

	it1 := table.NewConcatIterator([]table.Table{t1}, false)
	it2 := table.NewConcatIterator([]table.Table{t2}, false)
	it := table.NewMergeIterator([]table.Iterator{it1, it2}, false)
	defer it.Close()

	it.Rewind()
	require.True(t, it.Valid())
	k := it.Key()
	require.EqualValues(t, "k1", string(k))
	vs := it.Value()
	require.EqualValues(t, "a1", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()

	require.True(t, it.Valid())
	k = it.Key()
	require.EqualValues(t, "k2", string(k))
	vs = it.Value()
	require.EqualValues(t, "a2", string(vs.Value))
	require.EqualValues(t, 'A', vs.Meta)
	it.Next()
	require.True(t, it.Valid())

	k = it.Key()
	require.EqualValues(t, "l1", string(k))
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
	b, f := newTableBuilderForTest(t)
	for _, key := range keys {
		b.Add(key, &y.ValueStruct{Value: key, Meta: 'A', UserMeta: []byte{0}})
	}
	_, err := b.Finish(f.Name(), f)
	y.Check(err)
	y.Check(f.Close())
	f, _ = y.OpenSyncedFile(f.Name(), true)
	t1, err := OpenTestTable(f.Name(), nil)

	require.NoError(t, err)
	defer t1.Delete()
	it := t1.NewIterator(false).(*Iterator)
	defer it.Close()
	for i := 1; i < it.t.idx.numBlocks(); i++ {
		baseKey := []byte(string(it.t.idx.commonPrefix) + string(it.t.idx.blockDiffKey(i)))
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
	inMemTbl, err := OpenTable(NewInMemFile(1, blockData), nil)
	require.Nil(t, err)
	fileTable, err := OpenTestTable(file.Name(), nil)
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
	b := NewTableBuilder(1, defaultBuilderOpt)
	keyValues := generateKeyValues("in-mem", 1000)
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for _, kv := range keyValues {
		y.Assert(len(kv) == 2)
		b.Add([]byte(kv[0]), &y.ValueStruct{Value: []byte(kv[1]), Meta: 'A', UserMeta: []byte{0}})
	}
	buffer := bytes.NewBuffer(nil)
	_, err := b.Finish("", buffer)
	y.Check(err)
	require.NotNil(t, buffer.Bytes())
	tbl, err := OpenTable(NewInMemFile(1, buffer.Bytes()), nil)
	y.Check(err)
	for _, kv := range keyValues {
		key := []byte(kv[0])
		keyHash := farm.Fingerprint64(key)
		v, err := tbl.Get(key, 0, keyHash)
		require.Nil(t, err)
		require.EqualValues(t, v.Value, []byte(kv[1]))
	}
}

func BenchmarkRead(b *testing.B) {
	n := 5 << 20
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
	f, err := y.OpenSyncedFile(filename, true)
	y.Check(err)
	builder := NewTableBuilder(1, defaultBuilderOpt)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%016x", i)
		v := fmt.Sprintf("%d", i)
		builder.Add([]byte(k), &y.ValueStruct{Value: []byte(v), Meta: 123, UserMeta: []byte{0}})
	}
	_, err = builder.Finish(f.Name(), f)
	y.Check(err)
	tbl, err := OpenTestTable(f.Name(), nil)
	y.Check(err)
	defer tbl.Delete()

	//	y.Printf("Size of table: %d\n", tbl.Size())
	b.ResetTimer()
	// Iterate b.N times over the entire table.
	for i := 0; i < b.N; i++ {
		func() {
			it := tbl.NewIterator(false)
			for it.Rewind(); it.Valid(); it.Next() {
			}
			it.Close()
		}()
	}
}

func BenchmarkBuildTable(b *testing.B) {
	ns := []int{1000, 10000, 100000, 1000000, 5000000, 10000000, 15000000}
	for _, n := range ns {
		kvs := make([]struct {
			k []byte
			v []byte
		}, n)
		for i := 0; i < n; i++ {
			kvs[i].k = []byte(fmt.Sprintf("%016x", i))
			kvs[i].v = []byte(fmt.Sprintf("%d", i))
		}
		b.ResetTimer()

		b.Run(fmt.Sprintf("NoHash_%d", n), func(b *testing.B) {
			filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
			f, err := y.OpenSyncedFile(filename, false)
			y.Check(err)
			opt := defaultBuilderOpt
			for bn := 0; bn < b.N; bn++ {
				builder := NewTableBuilder(1, opt)
				for i := 0; i < n; i++ {
					builder.Add(kvs[i].k, &y.ValueStruct{Value: kvs[i].v, Meta: 123, UserMeta: []byte{0}})
				}
				_, err = builder.Finish(f.Name(), f)
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
				builder := NewTableBuilder(1, defaultBuilderOpt)
				for i := 0; i < n; i++ {
					builder.Add(kvs[i].k, &y.ValueStruct{Value: kvs[i].v, Meta: 123, UserMeta: []byte{0}})
				}
				_, err = builder.Finish(f.Name(), f)
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
	ns := []int{1000, 10000, 100000, 1000000, 5000000}
	for _, n := range ns {
		filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
		f, err := y.OpenSyncedFile(filename, true)
		builder := NewTableBuilder(uint64(n), defaultBuilderOpt)
		keys := make([][]byte, n)
		y.Check(err)
		for i := 0; i < n; i++ {
			k := []byte(fmt.Sprintf("%016x", i))
			v := fmt.Sprintf("%d", i)
			keys[i] = k
			builder.Add(k, &y.ValueStruct{Value: []byte(v), Meta: 'A', UserMeta: []byte{0}})
		}
		_, err = builder.Finish(f.Name(), f)
		y.Check(err)
		tbl, err := OpenTestTable(filename, nil)
		y.Check(err)
		b.ResetTimer()

		b.Run(fmt.Sprintf("Seek_%d", n), func(b *testing.B) {
			var vs y.ValueStruct
			rand := rand.New(rand.NewSource(0))
			for bn := 0; bn < b.N; bn++ {
				rand.Seed(0)
				for i := 0; i < n; i++ {
					k := keys[rand.Intn(n)]
					it := tbl.NewIterator(false)
					it.Seek(k)
					if !it.Valid() {
						it.Close()
						continue
					}
					if !bytes.Equal(k, it.Key()) {
						it.Close()
						continue
					}
					vs = it.Value()
					it.Close()
				}
			}
			_ = vs
		})

		b.Run(fmt.Sprintf("Hash_%d", n), func(b *testing.B) {
			var resultVs y.ValueStruct
			rand := rand.New(rand.NewSource(0))
			for bn := 0; bn < b.N; bn++ {
				rand.Seed(0)
				for i := 0; i < n; i++ {
					k := keys[rand.Intn(n)]
					keyHash := farm.Fingerprint64(k)
					resultVs, _ = tbl.Get(k, 0, keyHash)
				}
			}
			_ = resultVs
		})

		tbl.Delete()
	}
}

func BenchmarkReadAndBuild(b *testing.B) {
	n := 5 << 20
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
	f, err := y.OpenSyncedFile(filename, false)
	builder := NewTableBuilder(1, defaultBuilderOpt)
	y.Check(err)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%016x", i))
		v := fmt.Sprintf("%d", i)
		builder.Add(k, &y.ValueStruct{Value: []byte(v), Meta: 'A', UserMeta: []byte{0}})
	}
	_, err = builder.Finish(f.Name(), f)
	y.Check(err)
	tbl, err := OpenTestTable(f.Name(), nil)
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
			newBuilder := NewTableBuilder(1, TableBuilderOptions{})
			it := tbl.NewIterator(false)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				vs := it.Value()
				newBuilder.Add(it.Key(), &vs)
			}
			_, err = newBuilder.Finish(f.Name(), f)
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
		builder := NewTableBuilder(uint64(i+1), defaultBuilderOpt)
		for j := 0; j < tableSize; j++ {
			id := j*m + i // Arrays are interleaved.
			// id := i*tableSize+j (not interleaved)
			k := []byte(fmt.Sprintf("%016x", id))
			v := fmt.Sprintf("%d", id)
			builder.Add(k, &y.ValueStruct{Value: []byte(v), Meta: 'A', UserMeta: []byte{0}})
		}
		_, err = builder.Finish(f.Name(), f)
		y.Check(err)
		tbl, err := OpenTestTable(f.Name(), nil)
		y.Check(err)
		tables = append(tables, tbl)
		defer tbl.Delete()
	}

	b.ResetTimer()
	// Iterate b.N times over the entire table.
	for i := 0; i < b.N; i++ {
		func() {
			var iters []table.Iterator
			for _, tbl := range tables {
				iters = append(iters, tbl.NewIterator(false))
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
	tbl := getTableForBenchmarks(b, n)

	r := rand.New(rand.NewSource(time.Now().Unix()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		itr := tbl.NewIterator(false)
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

func getTableForBenchmarks(b *testing.B, count int) *Table {
	rand.Seed(time.Now().Unix())
	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Uint32())
	f, err := y.OpenSyncedFile(filename, false)
	builder := NewTableBuilder(1, defaultBuilderOpt)
	require.NoError(b, err)
	for i := 0; i < count; i++ {
		k := []byte(fmt.Sprintf("%016x", i))
		v := fmt.Sprintf("%d", i)
		builder.Add(k, &y.ValueStruct{Value: []byte(v)})
	}

	_, err = builder.Finish(f.Name(), f)
	require.NoError(b, err, "unable to write to file")
	tbl, err := OpenTestTable(f.Name(), nil)
	require.NoError(b, err, "unable to open table")
	return tbl
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UTC().UnixNano())
	os.Exit(m.Run())
}
