package memtable

import (
	"fmt"
	"github.com/pingcap/badger/y"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestListNodeIterator(t *testing.T) {
	var entries []Entry
	for i := 0; i < 100; i++ {
		numVer := i%10 + 1
		for j := 0; j < numVer; j++ {
			entries = append(entries, newTestEntry(newKey(i), 10-j))
		}
	}
	ln := newListNode(entries)
	it := ln.newIterator(true)
	require.Equal(t, len(ln.latestOffs), 100)
	for i := 0; i < 100; i++ {
		key := newKey(i)
		it.Seek(key)
		require.EqualValues(t, it.Key().UserKey, key)
		numVer := i%10 + 1
		require.True(t, it.Key().Version == 10)
		for j := 1; j < numVer; j++ {
			require.True(t, it.NextVersion())
			require.True(t, it.Key().Version == uint64(10-j))
		}
	}
	it.Rewind()
	for i := 98; i >= 0; i-- {
		it.Next()
		key := newKey(i)
		require.EqualValues(t, it.Key().UserKey, key)
	}
	it.Close()
	it = ln.newIterator(false)
	for i := 1; i < 100; i++ {
		it.Next()
		key := newKey(i)
		require.EqualValues(t, it.Key().UserKey, key)
	}
	it.Close()
}

func newKey(i int) []byte {
	return []byte(fmt.Sprintf("key%.3d", i))
}

func newTestEntry(key []byte, version int) Entry {
	return Entry{
		Key: key,
		Value: y.ValueStruct{
			Value:   key,
			Version: uint64(version),
		},
	}
}
