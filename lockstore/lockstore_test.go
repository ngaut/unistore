package lockstore

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemStore(t *testing.T) {
	prefix := "ls"
	n := 30000
	ls := NewMemStore(1 << 10)
	val := ls.Get([]byte("a"), nil)
	require.Len(t, val, 0)

	insertMemStore(ls, prefix, n)
	numBlocks := len(ls.getArena().blocks)
	checkMemStore(t, ls, prefix, n)
	deleteMemStore(t, ls, prefix, n)
	require.Equal(t, len(ls.getArena().blocks), numBlocks)
	time.Sleep(reuseSafeDuration)
	insertMemStore(ls, prefix, n)
	// Because the height is random, we insert again, the block number may be different.
	diff := len(ls.getArena().blocks) - numBlocks
	require.True(t, diff < numBlocks/100)
	require.Len(t, ls.Get(numToKey(n), nil), 0)
	require.Len(t, ls.Get([]byte("abc"), nil), 0)
}

const keyFormat = "%s%020d"

func insertMemStore(ls *MemStore, prefix string, n int) *MemStore {
	perms := rand.Perm(n)
	for _, v := range perms {
		key := []byte(fmt.Sprintf(keyFormat, prefix, v))
		ls.Insert(key, key)
	}
	return ls
}

func checkMemStore(t *testing.T, ls *MemStore, prefix string, n int) {
	perms := rand.Perm(n)
	for _, v := range perms {
		key := []byte(fmt.Sprintf(keyFormat, prefix, v))
		val := ls.Get(key, nil)
		require.True(t, bytes.Equal(key, val))
	}
}

func deleteMemStore(t *testing.T, ls *MemStore, prefix string, n int) {
	perms := rand.Perm(n)
	for _, v := range perms {
		key := []byte(fmt.Sprintf(keyFormat, prefix, v))
		require.True(t, ls.Delete(key), string(key))
	}
}

func TestIterator(t *testing.T) {
	ls := NewMemStore(1 << 10)
	for i := 10; i < 1000; i += 10 {
		key := []byte(fmt.Sprintf(keyFormat, "ls", i))
		ls.Insert(key, bytes.Repeat(key, 10))
	}
	require.Len(t, ls.getArena().blocks, 33)
	it := ls.NewIterator()
	it.SeekToFirst()
	checkKey(t, it, 10)
	it.Next()
	checkKey(t, it, 20)
	it.SeekToFirst()
	checkKey(t, it, 10)
	it.SeekToLast()
	checkKey(t, it, 990)
	it.Seek(numToKey(11))
	checkKey(t, it, 20)
	it.Seek(numToKey(989))
	checkKey(t, it, 990)
	it.Seek(numToKey(0))
	checkKey(t, it, 10)

	it.Seek(numToKey(2000))
	require.False(t, it.Valid())

	it.Seek(numToKey(500))
	checkKey(t, it, 500)
	it.Prev()
	checkKey(t, it, 490)
	it.SeekForPrev(numToKey(100))
	checkKey(t, it, 100)
	it.SeekForPrev(numToKey(99))
	checkKey(t, it, 90)

	it.SeekForPrev(numToKey(2000))
	checkKey(t, it, 990)
}

func checkKey(t *testing.T, it *Iterator, n int) {
	require.True(t, it.Valid())
	require.True(t, bytes.Equal(it.Key(), []byte(fmt.Sprintf(keyFormat, "ls", n))))
	require.True(t, bytes.Equal(it.Value(), bytes.Repeat(it.Key(), 10)))
}

func numToKey(n int) []byte {
	return []byte(fmt.Sprintf(keyFormat, "ls", n))
}

func TestConcurrent(t *testing.T) {
	keyRange := 10
	concurrentKeys := make([][]byte, keyRange)
	for i := 0; i < keyRange; i++ {
		concurrentKeys[i] = numToKey(i)
	}

	ls := NewMemStore(1 << 20)
	// Starts 10 readers and 1 writer.
	closeCh := make(chan bool)
	for i := 0; i < keyRange; i++ {
		go runReader(ls, closeCh, i)
	}
	ran := rand.New(rand.NewSource(time.Now().Unix()))
	start := time.Now()
	var totalInsert, totalDelete int
	for {
		if totalInsert%128 == 0 && time.Since(start) > time.Second*10 {
			break
		}
		n := ran.Intn(keyRange)
		key := concurrentKeys[n]
		if ls.Insert(key, key) {
			totalInsert++
		}
		n = ran.Intn(keyRange)
		key = concurrentKeys[n]
		if ls.Delete(key) {
			totalDelete++
		}
	}
	close(closeCh)
	time.Sleep(time.Millisecond * 100)
	arena := ls.getArena()
	fmt.Println("total insert", totalInsert, "total delete", totalDelete)
	fmt.Println(len(arena.pendingBlocks), len(arena.writableQueue), len(arena.blocks))
}

func runReader(ls *MemStore, closeCh chan bool, i int) {
	key := numToKey(i)
	buf := make([]byte, 100)
	var n int
	for {
		n++
		if n%128 == 0 {
			select {
			case <-closeCh:
				fmt.Println("read", n)
				return
			default:
			}
		}
		result := ls.Get(key, buf)
		if len(result) > 0 && !bytes.Equal(key, result) {
			panic("data corruption")
		}
	}
}

func BenchmarkMemStoreDeleteInsertGet(b *testing.B) {
	ls := NewMemStore(1 << 23)
	keys := make([][]byte, 10000)
	for i := 0; i < 10000; i++ {
		keys[i] = numToKey(i)
		ls.Insert(keys[i], keys[i])
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	buf := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := r.Intn(10000)
		ls.Delete(keys[n])
		ls.Insert(keys[n], keys[n])
		ls.Get(keys[n], buf)
	}
}

func BenchmarkMemStoreIterate(b *testing.B) {
	ls := NewMemStore(1 << 23)
	keys := make([][]byte, 10000)
	for i := 0; i < 10000; i++ {
		keys[i] = numToKey(i)
		ls.Insert(keys[i], keys[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := ls.NewIterator()
		it.SeekToFirst()
		for it.Valid() {
			it.Next()
		}
	}
}
