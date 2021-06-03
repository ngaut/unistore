package memtable

import (
	"github.com/ngaut/unistore/sdbpb"
	"github.com/pingcap/badger/y"
	"sync/atomic"
)

type Entry struct {
	Key   []byte
	Value y.ValueStruct
}

func (e *Entry) EstimateSize() int64 {
	return int64(len(e.Key) + int(e.Value.EncodedSize()) + EstimateNodeSize)
}

type Table struct {
	skls    []*skiplist
	hints   []Hint
	arena   *arena
	version uint64
	props   *sdbpb.Properties
	stage   int32
}

func NewCFTable(numCFs int) *Table {
	t := &Table{
		skls:  make([]*skiplist, numCFs),
		hints: make([]Hint, numCFs),
		arena: newArena(),
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

func (cft *Table) Put(cf int, key []byte, val y.ValueStruct) {
	cft.skls[cf].Put(key, val)
}

func (cft *Table) PutEntries(cf int, entries []*Entry) {
	skl := cft.skls[cf]
	for _, entry := range entries {
		skl.PutWithHint(entry.Key, entry.Value, &cft.hints[cf])
	}
}

func (cft *Table) Size() int64 {
	return cft.arena.size()
}

func (cft *Table) Get(cf int, key []byte, version uint64) y.ValueStruct {
	return cft.skls[cf].Get(key, version)
}

func (cft *Table) GetWithHint(cf int, key []byte, version uint64, hint *Hint) y.ValueStruct {
	return cft.skls[cf].GetWithHint(key, version, hint)
}

func (cft *Table) DeleteKey(cf byte, key []byte) bool {
	return cft.skls[cf].DeleteKey(key)
}

func (cft *Table) NewIterator(cf int, reversed bool) *UniIterator {
	if cft.skls[cf].Empty() {
		return nil
	}
	return cft.skls[cf].NewUniIterator(reversed)
}

func (cft *Table) Empty() bool {
	for _, skl := range cft.skls {
		if !skl.Empty() {
			return false
		}
	}
	return true
}

func (cft *Table) SetVersion(version uint64) {
	atomic.StoreUint64(&cft.version, version)
}

func (cft *Table) GetVersion() uint64 {
	return atomic.LoadUint64(&cft.version)
}

func (cft *Table) SetProps(props *sdbpb.Properties) {
	cft.props = props
}

func (cft *Table) GetProps() *sdbpb.Properties {
	return cft.props
}

func (cft *Table) SetSplitStage(stage sdbpb.SplitStage) {
	atomic.StoreInt32(&cft.stage, int32(stage))
}

func (cft *Table) GetSplitStage() sdbpb.SplitStage {
	return sdbpb.SplitStage(atomic.LoadInt32(&cft.stage))
}
