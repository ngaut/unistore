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

package table

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/pingcap/badger/cache/z"
	"github.com/pingcap/badger/y"
	"github.com/stretchr/testify/require"
)

type SimpleIterator struct {
	keys     []y.Key
	vals     [][]byte
	idx      int
	reversed bool

	latestOffs []int
	verIdx     int
}

var (
	closeCount int
)

func (s *SimpleIterator) Next() {
	if !s.reversed {
		s.idx++
		s.verIdx = 0
	} else {
		s.idx--
		s.verIdx = 0
	}
}

func (s *SimpleIterator) NextVersion() bool {
	nextEntryOff := len(s.keys)
	if s.idx+1 < len(s.latestOffs) {
		nextEntryOff = s.latestOffs[s.idx+1]
	}
	if s.entryIdx()+1 < nextEntryOff {
		s.verIdx++
		return true
	}
	return false
}

func (s *SimpleIterator) Rewind() {
	if !s.reversed {
		s.idx = 0
		s.verIdx = 0
	} else {
		s.idx = len(s.latestOffs) - 1
		s.verIdx = 0
	}
}

func (s *SimpleIterator) Seek(key []byte) {
	s.idx = sort.Search(len(s.latestOffs), func(i int) bool {
		return bytes.Compare(s.keys[s.latestOffs[i]].UserKey, key) >= 0
	})
	s.verIdx = 0
	if s.reversed {
		if !s.Valid() || !bytes.Equal(s.Key().UserKey, key) {
			s.idx--
		}
	}
}

func (s *SimpleIterator) Key() y.Key {
	return s.keys[s.entryIdx()]
}

func (s *SimpleIterator) entryIdx() int {
	return s.latestOffs[s.idx] + s.verIdx
}

func (s *SimpleIterator) Value() y.ValueStruct {
	return y.ValueStruct{
		Value:    s.vals[s.entryIdx()],
		UserMeta: []byte{55},
		Meta:     0,
	}
}
func (s *SimpleIterator) FillValue(vs *y.ValueStruct) {
	vs.Value = s.vals[s.entryIdx()]
	vs.UserMeta = []byte{55} // arbitrary value for test
	vs.Meta = 0
}
func (s *SimpleIterator) Valid() bool {
	return s.idx >= 0 && s.idx < len(s.latestOffs)
}

func (s *SimpleIterator) Close() error {
	return nil
}

func newSimpleIterator(keys []string, vals []string, reversed bool) *SimpleIterator {
	k := make([]y.Key, len(keys))
	v := make([][]byte, len(vals))
	lastestOffs := make([]int, len(keys))
	y.Assert(len(keys) == len(vals))
	for i := 0; i < len(keys); i++ {
		k[i] = y.KeyWithTs([]byte(keys[i]), 0)
		v[i] = []byte(vals[i])
		lastestOffs[i] = i
	}
	return &SimpleIterator{
		keys:       k,
		vals:       v,
		idx:        -1,
		reversed:   reversed,
		latestOffs: lastestOffs,
	}
}

func newMultiVersionSimpleIterator(maxVersion, minVersion uint64, reversed bool) *SimpleIterator {
	var latestOffs []int
	var keys []y.Key
	var vals [][]byte
	for i := 0; i < 100; i++ {
		latestOffs = append(latestOffs, len(keys))
		key := []byte(fmt.Sprintf("key%.3d", i))
		for j := maxVersion; j >= minVersion; j-- {
			keys = append(keys, y.KeyWithTs(key, j))
			vals = append(vals, key)
			if z.FastRand()%4 == 0 {
				break
			}
		}
	}
	return &SimpleIterator{
		keys:       keys,
		vals:       vals,
		idx:        0,
		reversed:   reversed,
		latestOffs: latestOffs,
	}
}

func getAll(it y.Iterator) ([]string, []string) {
	var keys, vals []string
	for ; it.Valid(); it.Next() {
		k := it.Key()
		keys = append(keys, string(k.UserKey))
		v := it.Value()
		vals = append(vals, string(v.Value))
	}
	return keys, vals
}

func TestSimpleIterator(t *testing.T) {
	keys := []string{"1", "2", "3"}
	vals := []string{"v1", "v2", "v3"}
	it := newSimpleIterator(keys, vals, false)
	defer it.Close()
	it.Rewind()
	k, v := getAll(it)
	require.EqualValues(t, keys, k)
	require.EqualValues(t, vals, v)
}

func reversed(a []string) []string {
	var out []string
	for i := len(a) - 1; i >= 0; i-- {
		out = append(out, a[i])
	}
	return out
}

func TestMergeSingle(t *testing.T) {
	keys := []string{"1", "2", "3"}
	vals := []string{"v1", "v2", "v3"}
	it := newSimpleIterator(keys, vals, false)
	mergeIt := NewMergeIterator([]y.Iterator{it}, false)
	defer mergeIt.Close()
	mergeIt.Rewind()
	k, v := getAll(mergeIt)
	require.EqualValues(t, keys, k)
	require.EqualValues(t, vals, v)
}

func TestMergeSingleReversed(t *testing.T) {
	keys := []string{"1", "2", "3"}
	vals := []string{"v1", "v2", "v3"}
	it := newSimpleIterator(keys, vals, true)
	mergeIt := NewMergeIterator([]y.Iterator{it}, true)
	defer mergeIt.Close()
	mergeIt.Rewind()
	k, v := getAll(mergeIt)
	require.EqualValues(t, reversed(keys), k)
	require.EqualValues(t, reversed(vals), v)
}

func TestMergeMore(t *testing.T) {
	it1 := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"}, false)
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"}, false)
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"}, false)
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"}, false)

	mergeIt := NewMergeIterator([]y.Iterator{it1, it2, it3, it4}, false)
	defer mergeIt.Close()
	expectedKeys := []string{"1", "2", "3", "5", "7", "9"}
	expectedVals := []string{"a1", "b2", "a3", "b5", "a7", "d9"}
	mergeIt.Rewind()
	k, v := getAll(mergeIt)
	require.EqualValues(t, expectedKeys, k)
	require.EqualValues(t, expectedVals, v)
}

// Ensure MergeIterator satisfies the Iterator interface
func TestMergeIteratorNested(t *testing.T) {
	keys := []string{"1", "2", "3"}
	vals := []string{"v1", "v2", "v3"}
	it := newSimpleIterator(keys, vals, false)
	mergeIt := NewMergeIterator([]y.Iterator{it}, false)
	mergeIt2 := NewMergeIterator([]y.Iterator{mergeIt}, false)
	defer mergeIt2.Close()
	mergeIt2.Rewind()
	k, v := getAll(mergeIt2)
	require.EqualValues(t, keys, k)
	require.EqualValues(t, vals, v)
}

func TestMergeIteratorSeek(t *testing.T) {
	it := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"}, false)
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"}, false)
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"}, false)
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"}, false)
	mergeIt := NewMergeIterator([]y.Iterator{it, it2, it3, it4}, false)
	defer mergeIt.Close()
	mergeIt.Seek([]byte("4"))
	k, v := getAll(mergeIt)
	require.EqualValues(t, []string{"5", "7", "9"}, k)
	require.EqualValues(t, []string{"b5", "a7", "d9"}, v)
}

func TestMergeIteratorSeekReversed(t *testing.T) {
	it := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"}, true)
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"}, true)
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"}, true)
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"}, true)
	mergeIt := NewMergeIterator([]y.Iterator{it, it2, it3, it4}, true)
	defer mergeIt.Close()
	mergeIt.Seek([]byte("5"))
	k, v := getAll(mergeIt)
	require.EqualValues(t, []string{"5", "3", "2", "1"}, k)
	require.EqualValues(t, []string{"b5", "a3", "b2", "a1"}, v)
}

func TestMergeIteratorSeekInvalid(t *testing.T) {
	it := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"}, false)
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"}, false)
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"}, false)
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"}, false)
	mergeIt := NewMergeIterator([]y.Iterator{it, it2, it3, it4}, false)
	defer mergeIt.Close()
	mergeIt.Seek([]byte("f"))
	require.False(t, mergeIt.Valid())
}

func TestMergeIteratorSeekInvalidReversed(t *testing.T) {
	it := newSimpleIterator([]string{"1", "3", "7"}, []string{"a1", "a3", "a7"}, true)
	it2 := newSimpleIterator([]string{"2", "3", "5"}, []string{"b2", "b3", "b5"}, true)
	it3 := newSimpleIterator([]string{"1"}, []string{"c1"}, true)
	it4 := newSimpleIterator([]string{"1", "7", "9"}, []string{"d1", "d7", "d9"}, true)
	mergeIt := NewMergeIterator([]y.Iterator{it, it2, it3, it4}, true)
	defer mergeIt.Close()
	mergeIt.Seek([]byte("0"))
	require.False(t, mergeIt.Valid())
}

func TestMergeIteratorDuplicate(t *testing.T) {
	it1 := newSimpleIterator([]string{"0", "1", "2"}, []string{"0", "1", "2"}, false)
	it2 := newSimpleIterator([]string{"1"}, []string{"1"}, false)
	it3 := newSimpleIterator([]string{"2"}, []string{"2"}, false)
	it := NewMergeIterator([]y.Iterator{it3, it2, it1}, false)
	defer it.Close()

	var cnt int
	for it.Rewind(); it.Valid(); it.Next() {
		require.EqualValues(t, cnt+48, it.Key().UserKey[0])
		cnt++
	}
	require.Equal(t, 3, cnt)
}

func TestMultiVersionMergeIterator(t *testing.T) {
	for _, reverse := range []bool{true, false} {
		it1 := newMultiVersionSimpleIterator(100, 90, reverse)
		it2 := newMultiVersionSimpleIterator(90, 80, reverse)
		it3 := newMultiVersionSimpleIterator(80, 70, reverse)
		it4 := newMultiVersionSimpleIterator(70, 60, reverse)
		it := NewMergeIterator([]y.Iterator{it1, it2, it3, it4}, reverse)

		it.Rewind()
		curKey := it.Key().UserKey
		for i := 1; i < 100; i++ {
			it.Next()
			require.True(t, it.Valid())
			require.False(t, bytes.Equal(curKey, it.Key().UserKey))
			curKey = it.Key().UserKey
			curVer := it.Key().Version
			for it.NextVersion() {
				require.True(t, it.Key().Version < curVer)
				curVer = it.Key().Version
			}
		}
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key%.3d", z.FastRand()%100))
			it.Seek(key)
			require.True(t, it.Valid())
			require.EqualValues(t, it.Key().UserKey, key)
			curVer := it.Key().Version
			for it.NextVersion() {
				require.True(t, it.Key().Version < curVer)
				curVer = it.Key().Version
			}
			require.True(t, curVer <= 70)
		}
		it.Close()
	}
}

func BenchmarkMergeIterator(b *testing.B) {
	num := 2
	simpleIters := make([]y.Iterator, num)
	for i := 0; i < num; i++ {
		simpleIters[i] = new(SimpleIterator)
	}
	for i := 0; i < num*100; i += num {
		for j := 0; j < num; j++ {
			iter := simpleIters[j].(*SimpleIterator)
			iter.keys = append(iter.keys, y.KeyWithTs([]byte(fmt.Sprintf("key%08d", i+j)), 0))
		}
	}
	mergeIter := NewMergeIterator(simpleIters, false)
	defer mergeIter.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mergeIter.Rewind()
		for mergeIter.Valid() {
			mergeIter.Key()
			mergeIter.Next()
		}
	}
}
