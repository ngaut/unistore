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

package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ngaut/unistore/sdb/cache/z"
	"github.com/pingcap/badger/y"
	"github.com/stretchr/testify/require"
)

const arenaSize = 1 << 20

func newValue(v int) []byte {
	return []byte(fmt.Sprintf("%05d", v))
}

// length iterates over skiplist to give exact size.
func length(s *skiplist) int {
	x := s.getNext(s.head, 0)
	count := 0
	for x != nil {
		count++
		x = s.getNext(x, 0)
	}
	return count
}

func TestEmpty(t *testing.T) {
	key := []byte("aaa")
	l := newSkiplist(arenaSize)

	v := l.Get(key, 1)
	require.True(t, v.Value == nil) // Cannot use require.Nil for unsafe.Pointer nil.

	for _, less := range []bool{true, false} {
		for _, allowEqual := range []bool{true, false} {
			n, found := l.findNear(key, less, allowEqual)
			require.Nil(t, n)
			require.False(t, found)
		}
	}

	it := l.NewIterator()
	require.False(t, it.Valid())

	it.SeekToFirst()
	require.False(t, it.Valid())

	it.SeekToLast()
	require.False(t, it.Valid())

	it.Seek(key)
	require.False(t, it.Valid())
	it.Close()
}

// TestBasic tests single-threaded inserts and updates and gets.
func TestBasic(t *testing.T) {
	l := newSkiplist(arenaSize)
	val1 := newValue(42)
	val2 := newValue(52)
	val3 := newValue(62)
	val4 := newValue(72)

	// Try inserting values.
	// Somehow require.Nil doesn't work when checking for unsafe.Pointer(nil).
	l.Put([]byte("key1"), y.ValueStruct{Value: val1, Meta: 55, UserMeta: []byte{0}})
	l.Put([]byte("key2"), y.ValueStruct{Value: val2, Meta: 56, UserMeta: []byte{0}, Version: 2})
	l.Put([]byte("key3"), y.ValueStruct{Value: val3, Meta: 57, UserMeta: []byte{0}})

	v := l.Get([]byte("key"), 0)
	require.True(t, v.Value == nil)

	v = l.Get([]byte("key1"), 0)
	require.True(t, v.Value != nil)
	require.EqualValues(t, "00042", string(v.Value))
	require.EqualValues(t, 55, v.Meta)

	v = l.Get([]byte("key2"), 0)
	require.True(t, v.Value == nil)

	v = l.Get([]byte("key3"), 0)
	require.True(t, v.Value != nil)
	require.EqualValues(t, "00062", string(v.Value))
	require.EqualValues(t, 57, v.Meta)

	l.Put([]byte("key3"), y.ValueStruct{Value: val4, Meta: 12, UserMeta: []byte{0}, Version: 1})
	v = l.Get([]byte("key3"), 1)
	require.True(t, v.Value != nil)
	require.EqualValues(t, "00072", string(v.Value))
	require.EqualValues(t, 12, v.Meta)
}

// TestConcurrentBasic tests concurrent writes followed by concurrent reads.
// The concurrent write operation is not supported.
func testConcurrentBasic(t *testing.T) {
	const n = 1000
	l := newSkiplist(arenaSize)
	var wg sync.WaitGroup
	key := func(i int) []byte {
		return []byte(fmt.Sprintf("%05d", i))
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l.Put(key(i),
				y.ValueStruct{Value: newValue(i), Meta: 0, UserMeta: []byte{0}})
		}(i)
	}
	wg.Wait()
	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := l.Get(key(i), 0)
			require.True(t, v.Value != nil)
			require.EqualValues(t, newValue(i), v.Value)
		}(i)
	}
	wg.Wait()
	require.EqualValues(t, n, length(l))
}

func TestFindNear(t *testing.T) {
	l := newSkiplist(arenaSize)
	defer l.Delete()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%05d", i*10+5)
		l.Put([]byte(key), y.ValueStruct{Value: newValue(i), Meta: 0, UserMeta: []byte{0}})
	}

	n, eq := l.findNear([]byte("00001"), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("00005"), n.key(l.arena))
	require.False(t, eq)
	n, eq = l.findNear([]byte("00001"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("00005"), n.key(l.arena))
	require.False(t, eq)
	n, eq = l.findNear([]byte("00001"), true, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("00001"), true, true)
	require.Nil(t, n)
	require.False(t, eq)

	n, eq = l.findNear([]byte("00005"), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("00015"), n.key(l.arena))
	require.False(t, eq)
	n, eq = l.findNear([]byte("00005"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("00005"), n.key(l.arena))
	require.True(t, eq)
	n, eq = l.findNear([]byte("00005"), true, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("00005"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("00005"), n.key(l.arena))
	require.True(t, eq)

	n, eq = l.findNear([]byte("05555"), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("05565"), n.key(l.arena))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05555"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("05555"), n.key(l.arena))
	require.True(t, eq)
	n, eq = l.findNear([]byte("05555"), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("05545"), n.key(l.arena))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05555"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("05555"), n.key(l.arena))
	require.True(t, eq)

	n, eq = l.findNear([]byte("05558"), false, false)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("05565"), n.key(l.arena))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05558"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("05565"), n.key(l.arena))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05558"), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("05555"), n.key(l.arena))
	require.False(t, eq)
	n, eq = l.findNear([]byte("05558"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("05555"), n.key(l.arena))
	require.False(t, eq)

	n, eq = l.findNear([]byte("09995"), false, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("09995"), false, true)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("09995"), n.key(l.arena))
	require.True(t, eq)
	n, eq = l.findNear([]byte("09995"), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("09985"), n.key(l.arena))
	require.False(t, eq)
	n, eq = l.findNear([]byte("09995"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("09995"), n.key(l.arena))
	require.True(t, eq)

	n, eq = l.findNear([]byte("59995"), false, false)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("59995"), false, true)
	require.Nil(t, n)
	require.False(t, eq)
	n, eq = l.findNear([]byte("59995"), true, false)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("09995"), n.key(l.arena))
	require.False(t, eq)
	n, eq = l.findNear([]byte("59995"), true, true)
	require.NotNil(t, n)
	require.EqualValues(t, []byte("09995"), n.key(l.arena))
	require.False(t, eq)
}

// TestIteratorNext tests a basic iteration over all nodes from the beginning.
func TestIteratorNext(t *testing.T) {
	const n = 100
	l := newSkiplist(arenaSize)
	defer l.Delete()
	it := l.NewIterator()
	defer it.Close()
	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	for i := n - 1; i >= 0; i-- {
		l.Put([]byte(fmt.Sprintf("%05d", i)),
			y.ValueStruct{Value: newValue(i), Meta: 0, UserMeta: []byte{0}})
	}
	it.SeekToFirst()
	for i := 0; i < n; i++ {
		require.True(t, it.Valid())
		v := it.Value()
		require.EqualValues(t, newValue(i), v.Value)
		it.Next()
	}
	require.False(t, it.Valid())
}

// TestIteratorPrev tests a basic iteration over all nodes from the end.
func TestIteratorPrev(t *testing.T) {
	const n = 100
	l := newSkiplist(arenaSize)
	defer l.Delete()
	it := l.NewIterator()
	defer it.Close()
	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	for i := 0; i < n; i++ {
		l.Put([]byte(fmt.Sprintf("%05d", i)),
			y.ValueStruct{Value: newValue(i), Meta: 0, UserMeta: []byte{0}})
	}
	it.SeekToLast()
	for i := n - 1; i >= 0; i-- {
		require.True(t, it.Valid())
		v := it.Value()
		require.EqualValues(t, newValue(i), v.Value)
		it.Prev()
	}
	require.False(t, it.Valid())
}

// TestIteratorSeek tests Seek and SeekForPrev.
func TestIteratorSeek(t *testing.T) {
	const n = 100
	l := newSkiplist(arenaSize)
	defer l.Delete()

	it := l.NewIterator()
	defer it.Close()

	require.False(t, it.Valid())
	it.SeekToFirst()
	require.False(t, it.Valid())
	// 1000, 1010, 1020, ..., 1990.
	for i := n - 1; i >= 0; i-- {
		v := i*10 + 1000
		l.Put([]byte(fmt.Sprintf("%05d", i*10+1000)),
			y.ValueStruct{Value: newValue(v), Meta: 0, UserMeta: []byte{0}})
	}
	it.SeekToFirst()
	require.True(t, it.Valid())
	v := it.Value()
	require.EqualValues(t, "01000", v.Value)

	it.Seek([]byte("01000"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01000", v.Value)

	it.Seek([]byte("01005"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01010", v.Value)

	it.Seek([]byte("01010"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01010", v.Value)

	it.Seek([]byte("99999"))
	require.False(t, it.Valid())

	// Try SeekForPrev.
	it.SeekForPrev([]byte("00"))
	require.False(t, it.Valid())

	it.SeekForPrev([]byte("01000"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01000", v.Value)

	it.SeekForPrev([]byte("01005"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01000", v.Value)

	it.SeekForPrev([]byte("01010"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01010", v.Value)

	it.SeekForPrev([]byte("99999"))
	require.True(t, it.Valid())
	v = it.Value()
	require.EqualValues(t, "01990", v.Value)
}

func TestPutWithHint(t *testing.T) {
	l := newSkiplist(arenaSize)
	sp := new(hint)
	cnt := 0
	for {
		if l.arena.size() > arenaSize-256 {
			break
		}
		key := randomKey()
		l.PutWithHint(key, y.ValueStruct{Value: key}, sp)
		cnt++
	}
	it := l.NewIterator()
	defer it.Close()
	var lastKey []byte
	cntGot := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		require.True(t, bytes.Compare(lastKey, it.Key()) <= 0)
		require.True(t, bytes.Compare(it.Key(), it.Value().Value) == 0)
		cntGot++
		lastKey = y.Copy(it.Key())
	}
	require.True(t, cntGot == cnt)
}

func TestGetWithHint(t *testing.T) {
	rand.Seed(time.Now().Unix())
	l := newSkiplist(arenaSize)
	var keys [][]byte
	sp := new(hint)
	for {
		if l.arena.size() > arenaSize-256 {
			break
		}
		key := randomKey()
		keys = append(keys, key)
		l.PutWithHint(key, y.ValueStruct{Value: key}, sp)
	}
	h := new(hint)
	for _, key := range keys {
		bytes.Equal(l.GetWithHint(key, 0, h).Value, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	for _, key := range keys {
		bytes.Equal(l.GetWithHint(key, 0, h).Value, key)
	}
}

func TestPutLargeValue(t *testing.T) {
	l := newSkiplist(arenaSize)
	key := randomKey()
	val := make([]byte, 128*1024)
	l.Put(key, y.ValueStruct{Value: val})
	result := l.Get(key, 0)
	require.Equal(t, val, result.Value)
}

func TestMemoryGrow(t *testing.T) {
	l := newSkiplist(arenaSize)
	for size := 0; size < 2*blockSize; {
		key := randomKey()
		val := make([]byte, 128*1024)
		vs := y.ValueStruct{Value: val}
		l.Put(key, vs)
		result := l.Get(key, 0)
		require.Equal(t, val, result.Value)
		size += len(key) + int(vs.EncodedSize())
	}
	require.True(t, true, l.MemSize() > blockSize)
}

func key(prefix string, i int) string {
	return prefix + fmt.Sprintf("%04d", i)
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

func buildMultiVersionSkiopList(keyValues [][]string) *skiplist {
	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	skl := newSkiplist(arenaSize)
	for _, kv := range keyValues {
		y.Assert(len(kv) == 2)
		val := fmt.Sprintf("%s_%d", kv[1], 9)
		skl.Put([]byte(kv[0]), y.ValueStruct{Value: []byte(val), Meta: 'A', UserMeta: []byte{0}, Version: 9})
		for i := uint64(8); i > 0; i-- {
			if z.FastRand()%4 != 0 {
				val = fmt.Sprintf("%s_%d", kv[1], i)
				skl.Put([]byte(kv[0]), y.ValueStruct{Value: []byte(val), Meta: 'A', UserMeta: []byte{0}, Version: i})
			}
		}
	}
	return skl
}

func TestIterateMultiVersion(t *testing.T) {
	keyVals := generateKeyValues("key", 4000)
	skl := buildMultiVersionSkiopList(keyVals)
	it := skl.NewIterator()
	defer it.Close()
	var lastKey []byte
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if len(lastKey) > 0 {
			require.True(t, bytes.Compare(lastKey, it.Key()) < 0)
		}
		lastKey = y.Copy(it.Key())
	}
	for i := 0; i < 1000; i++ {
		id := int(z.FastRand() % 4000)
		k := []byte(key("key", id))
		ver := uint64(5 + z.FastRand()%5)
		val := skl.Get(k, ver)
		if val.Valid() {
			valStr := fmt.Sprintf("%d_%d", id, ver)
			require.Equal(t, valStr, string(val.Value))
		} else {
			it.Seek(k)
			if it.Valid() {
				require.True(t, bytes.Compare(it.Key(), k) >= 0)
				cpKey := y.Copy(it.Key())
				it.Prev()
				if it.Valid() {
					require.True(t, bytes.Compare(it.Key(), k) < 0, "%s %s %s", it.Key(), cpKey, k)
				}
			}
		}
	}
	revIt := skl.NewUniIterator(true)
	defer revIt.Close()
	lastKey = nil
	for revIt.Rewind(); revIt.Valid(); revIt.Next() {
		if len(lastKey) > 0 {
			require.Truef(t, bytes.Compare(lastKey, revIt.Key()) > 0, "%s %s", lastKey, revIt.Key())
		}
		lastKey = y.Copy(revIt.Key())
	}
	for i := 0; i < 1000; i++ {
		k := y.KeyWithTs([]byte(key("key", int(z.FastRand()%4000))), uint64(5+z.FastRand()%5))
		// reverse iterator never seek to the same key with smaller version.
		revIt.Seek(k.UserKey)
		if !revIt.Valid() {
			continue
		}
		require.True(t, bytes.Compare(revIt.Key(), k.UserKey) <= 0, "%s %s", revIt.Key(), k)
	}
}

func TestDeleteKey(t *testing.T) {
	skl := newSkiplist(arenaSize)
	for i := 0; i < 1000; i++ {
		skl.Put(newKey(i), y.ValueStruct{Value: newKey(i)})
	}
	for i := 0; i < 1000; i++ {
		r := rand.Intn(1000)
		skl.DeleteKey(newKey(r))
	}
}

func randomKey() []byte {
	b := make([]byte, 8)
	key := rand.Uint32()
	key2 := rand.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWrite(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			l := newSkiplist(int64((b.N + 1) * MaxNodeSize))
			defer l.Delete()
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for pb.Next() {
					if rng.Float32() < readFrac {
						v := l.Get(randomKey(), 0)
						if v.Value != nil {
							count++
						}
					} else {
						l.Put(randomKey(), y.ValueStruct{Value: value, Meta: 0, UserMeta: []byte{0}})
					}
				}
			})
		})
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkReadWriteMap(b *testing.B) {
	value := newValue(123)
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i), func(b *testing.B) {
			m := make(map[string][]byte)
			var mutex sync.RWMutex
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if rand.Float32() < readFrac {
						mutex.RLock()
						_, ok := m[string(randomKey())]
						mutex.RUnlock()
						if ok {
							count++
						}
					} else {
						mutex.Lock()
						m[string(randomKey())] = value
						mutex.Unlock()
					}
				}
			})
		})
	}
}

// Standard test. Some fraction is read. Some fraction is write. Writes have
// to go through mutex lock.
func BenchmarkGetSequential(b *testing.B) {
	size := 300000
	keys, l, _ := buildKeysAndList(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%size]
		l.Get(key, 0)
	}
}

func BenchmarkGetWithHintSequential(b *testing.B) {
	size := 300000
	keys, l, h := buildKeysAndList(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%size]
		l.GetWithHint(key, 0, h)
	}
}

func buildKeysAndList(size int) ([][]byte, *skiplist, *hint) {
	l := newSkiplist(32 * 1024 * 1024)
	keys := make([][]byte, size)
	h := new(hint)
	for i := 0; i < size; i++ {
		keys[i] = []byte(fmt.Sprintf("%05d", i))
	}
	for i := 0; i < size; i++ {
		key := keys[i]
		l.PutWithHint(key, y.ValueStruct{Value: []byte{byte(i)}}, h)
	}
	return keys, l, h
}

func BenchmarkGetRandom(b *testing.B) {
	size := 300000
	keys, l, _ := buildKeysAndList(size)
	b.ResetTimer()
	r := rand.New(rand.NewSource(1))
	for i := 0; i < b.N; i++ {
		key := keys[r.Int()%size]
		l.Get(key, 0)
	}
}

func BenchmarkGetWithHintRandom(b *testing.B) {
	size := 300000
	keys, l, h := buildKeysAndList(size)
	b.ResetTimer()
	r := rand.New(rand.NewSource(1))
	for i := 0; i < b.N; i++ {
		key := keys[r.Int()%size]
		l.GetWithHint(key, 0, h)
	}
}

func BenchmarkPutWithHint(b *testing.B) {
	l := newSkiplist(16 * 1024 * 1024)
	size := 100000
	keys := make([][]byte, size)
	for i := 0; i < size; i++ {
		keys[i] = []byte(fmt.Sprintf("%05d", i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := new(hint)
		l = newSkiplist(16 * 1024 * 1024)
		for j := 0; j < size; j++ {
			key := keys[j]
			l.PutWithHint(key, y.ValueStruct{Value: []byte{byte(j)}}, h)
		}
	}
}
