package memtable

import (
	"bytes"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/ngaut/unistore/sdb/table"
	"github.com/pingcap/badger/y"
)

type Entry struct {
	Key   []byte
	Value y.ValueStruct
}

func (e *Entry) EstimateSize() int64 {
	return int64(len(e.Key) + int(e.Value.EncodedSize()) + EstimateNodeSize)
}

type Table struct {
	skl         *skiplist
	id          uint64
	pendingList unsafe.Pointer // *listNode
}

func New(arenaSize int64, id uint64) *Table {
	return &Table{
		skl: newSkiplist(arenaSize),
		id:  id,
	}
}

func (t *Table) ID() uint64 {
	return t.id
}

func (t *Table) Delete() error {
	t.skl.Delete()
	return nil
}

func (t *Table) Close() error {
	return nil
}

func (t *Table) Empty() bool {
	return atomic.LoadPointer(&t.pendingList) == nil && t.skl.Empty()
}

func (t *Table) Get(key y.Key, keyHash uint64) (y.ValueStruct, error) {
	curr := (*listNode)(atomic.LoadPointer(&t.pendingList))
	for curr != nil {
		if v, ok := curr.get(key); ok {
			return v, nil
		}
		curr = (*listNode)(atomic.LoadPointer(&curr.next))
	}
	return t.skl.Get(key.UserKey, key.Version), nil
}

func (t *Table) NewIterator(reverse bool) table.Iterator {
	var (
		sklItr = t.skl.NewUniIterator(reverse)
		its    []table.Iterator
	)
	curr := (*listNode)(atomic.LoadPointer(&t.pendingList))
	for curr != nil {
		its = append(its, curr.newIterator(reverse))
		curr = (*listNode)(atomic.LoadPointer(&curr.next))
	}

	if len(its) == 0 {
		return sklItr
	}
	its = append(its, sklItr)
	return table.NewMergeIterator(its, reverse)
}

func (t *Table) Size() int64 {
	var sz int64
	curr := (*listNode)(atomic.LoadPointer(&t.pendingList))
	for curr != nil {
		sz += curr.memSize
		curr = (*listNode)(atomic.LoadPointer(&curr.next))
	}
	return t.skl.MemSize() + sz
}

func (t *Table) Smallest() []byte {
	it := t.NewIterator(false)
	defer it.Close()
	it.Rewind()
	return it.Key()
}

func (t *Table) Biggest() []byte {
	it := t.NewIterator(true)
	defer it.Close()
	it.Rewind()
	return it.Key()
}

func (t *Table) HasOverlap(start, end []byte, includeEnd bool) bool {
	it := t.NewIterator(false)
	defer it.Close()
	it.Seek(start)
	if !it.Valid() {
		return false
	}
	if cmp := bytes.Compare(it.Key(), end); cmp > 0 {
		return false
	} else if cmp == 0 {
		return includeEnd
	}
	return true
}

// PutToSkl directly insert entry into SkipList.
func (t *Table) PutToSkl(key []byte, v y.ValueStruct) {
	t.skl.Put(key, v)
}

// PutToPendingList put entries to pending list, and you can call MergeListToSkl to merge them to SkipList later.
func (t *Table) PutToPendingList(entries []Entry) {
	t.putToList(entries)
}

// MergeListToSkl merge all entries in pending list to SkipList.
func (t *Table) MergeListToSkl() {
	head := (*listNode)(atomic.LoadPointer(&t.pendingList))
	if head == nil {
		return
	}

	head.mergeToSkl(t.skl)
	// No new node inserted, just update head of list.
	if atomic.CompareAndSwapPointer(&t.pendingList, unsafe.Pointer(head), nil) {
		return
	}
	// New node inserted, iterate to find `prev` of old head.
	curr := (*listNode)(atomic.LoadPointer(&t.pendingList))
	for curr != nil {
		next := atomic.LoadPointer(&curr.next)
		if unsafe.Pointer(head) == next {
			atomic.StorePointer(&curr.next, nil)
			return
		}
		curr = (*listNode)(next)
	}
}

func (t *Table) putToList(entries []Entry) {
	n := newListNode(entries)
	for {
		old := atomic.LoadPointer(&t.pendingList)
		n.next = old
		if atomic.CompareAndSwapPointer(&t.pendingList, old, unsafe.Pointer(n)) {
			return
		}
	}
}

type listNode struct {
	next       unsafe.Pointer // *listNode
	entries    []Entry
	latestOffs []uint32
	memSize    int64
}

func newListNode(entries []Entry) *listNode {
	n := &listNode{entries: entries, latestOffs: make([]uint32, 0, len(entries))}
	var curKey []byte
	for i, e := range n.entries {
		sz := e.EstimateSize()
		n.memSize += sz
		if !bytes.Equal(e.Key, curKey) {
			n.latestOffs = append(n.latestOffs, uint32(i))
			curKey = e.Key
		}
	}
	return n
}

func (n *listNode) putToSkl(s *skiplist, entries []Entry) {
	var h hint
	for _, e := range entries {
		s.PutWithHint(e.Key, e.Value, &h)
	}
}

func (n *listNode) mergeToSkl(skl *skiplist) {
	next := (*listNode)(atomic.LoadPointer(&n.next))
	if next != nil {
		next.mergeToSkl(skl)
	}
	atomic.StorePointer(&n.next, nil)
	n.putToSkl(skl, n.entries)
}

func (n *listNode) get(key y.Key) (y.ValueStruct, bool) {
	i := sort.Search(len(n.entries), func(i int) bool {
		e := n.entries[i]
		return y.KeyWithTs(e.Key, e.Value.Version).Compare(key) >= 0
	})
	if i < len(n.entries) && bytes.Equal(key.UserKey, n.entries[i].Key) {
		return n.entries[i].Value, true
	}
	return y.ValueStruct{}, false
}

func (n *listNode) newIterator(reverse bool) *listNodeIterator {
	return &listNodeIterator{reversed: reverse, n: n}
}

type listNodeIterator struct {
	idx      int
	verIdx   uint32
	n        *listNode
	reversed bool
}

func (it *listNodeIterator) Next() {
	if !it.reversed {
		it.idx++
		it.verIdx = 0
	} else {
		it.idx--
		it.verIdx = 0
	}
}

func (it *listNodeIterator) NextVersion() bool {
	nextKeyEntryOff := uint32(len(it.n.entries))
	if it.idx+1 < len(it.n.latestOffs) {
		nextKeyEntryOff = it.n.latestOffs[it.idx+1]
	}
	if it.getEntryIdx()+1 < nextKeyEntryOff {
		it.verIdx++
		return true
	}
	return false
}

func (it *listNodeIterator) getEntryIdx() uint32 {
	return it.n.latestOffs[it.idx] + it.verIdx
}

func (it *listNodeIterator) Rewind() {
	if !it.reversed {
		it.idx = 0
		it.verIdx = 0
	} else {
		it.idx = len(it.n.latestOffs) - 1
		it.verIdx = 0
	}
}

func (it *listNodeIterator) Seek(key []byte) {
	it.idx = sort.Search(len(it.n.latestOffs), func(i int) bool {
		e := &it.n.entries[it.n.latestOffs[i]]
		return bytes.Compare(e.Key, key) >= 0
	})
	it.verIdx = 0
	if it.reversed {
		if !it.Valid() || !bytes.Equal(it.Key(), key) {
			it.idx--
		}
	}
}

func (it *listNodeIterator) Key() []byte {
	e := &it.n.entries[it.getEntryIdx()]
	return e.Key
}

func (it *listNodeIterator) Value() y.ValueStruct { return it.n.entries[it.getEntryIdx()].Value }

func (it *listNodeIterator) FillValue(vs *y.ValueStruct) { *vs = it.Value() }

func (it *listNodeIterator) Valid() bool { return it.idx >= 0 && it.idx < len(it.n.latestOffs) }

func (it *listNodeIterator) Close() error { return nil }
