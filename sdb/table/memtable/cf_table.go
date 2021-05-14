package memtable

import (
	"github.com/pingcap/badger/y"
	"sync/atomic"
)

type CFTable struct {
	skls     []*skiplist
	arena    *arena
	version  uint64
	flushing uint32
}

func NewCFTable(arenaSize int64, numCFs int) *CFTable {
	t := &CFTable{
		skls:  make([]*skiplist, numCFs),
		arena: newArena(arenaSize),
	}
	for i := 0; i < numCFs; i++ {
		head := newNode(t.arena, nil, y.ValueStruct{}, maxHeight)
		t.skls[i] = &skiplist{
			height: 1,
			head:   head,
			arena:  t.arena,
			randX:  randSeed,
		}
	}
	return t
}

func (cft *CFTable) Put(cf int, key []byte, val y.ValueStruct) {
	cft.skls[cf].Put(key, val)
}

func (cft *CFTable) PutEntries(cf int, entries []*Entry) {
	var h hint
	skl := cft.skls[cf]
	for _, entry := range entries {
		skl.PutWithHint(entry.Key, entry.Value, &h)
	}
}

func (cft *CFTable) Size() int64 {
	return cft.arena.size()
}

func (cft *CFTable) Get(cf int, key []byte, version uint64) y.ValueStruct {
	return cft.skls[cf].Get(key, version)
}

func (cft *CFTable) DeleteKey(cf byte, key []byte) bool {
	return cft.skls[cf].DeleteKey(key)
}

func (cft *CFTable) NewIterator(cf int, reversed bool) *UniIterator {
	if cft.skls[cf].Empty() {
		return nil
	}
	return cft.skls[cf].NewUniIterator(reversed)
}

func (cft *CFTable) Empty() bool {
	for _, skl := range cft.skls {
		if !skl.Empty() {
			return false
		}
	}
	return true
}

func (cft *CFTable) SetVersion(version uint64) {
	atomic.StoreUint64(&cft.version, version)
}

func (cft *CFTable) GetVersion() uint64 {
	return atomic.LoadUint64(&cft.version)
}
