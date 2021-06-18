// Copyright 2021-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sdb

import (
	"bytes"
	"fmt"
	"github.com/ngaut/unistore/sdb/table"
	"github.com/pingcap/badger/y"
	"math"
)

// Item is returned during iteration. Both the Key() and Value() output is only valid until
// iterator.Next() is called.
type Item struct {
	key      []byte
	ver      uint64
	val      []byte
	meta     byte // We need to store meta to know about bitValuePointer.
	userMeta []byte
}

// String returns a string representation of Item
func (item *Item) String() string {
	return fmt.Sprintf("key=%q, version=%d, meta=%x", item.Key(), item.Version(), item.meta)
}

// Key returns the key.
//
// Key is only valid as long as item is valid, or transaction is valid.  If you need to use it
// outside its validity, please use KeyCopy
func (item *Item) Key() []byte {
	return item.key
}

// KeyCopy returns a copy of the key of the item, writing it to dst slice.
// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
// returned.
func (item *Item) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, item.key)
}

// Version returns the commit timestamp of the item.
func (item *Item) Version() uint64 {
	return item.ver
}

// IsEmpty checks if the value is empty.
func (item *Item) IsEmpty() bool {
	return len(item.val) == 0
}

// Value retrieves the value of the item from the value log.
//
// This method must be called within a transaction. Calling it outside a
// transaction is considered undefined behavior. If an iterator is being used,
// then Item.Value() is defined in the current iteration only, because items are
// reused.
//
// If you need to use a value outside a transaction, please use Item.ValueCopy
// instead, or copy it yourself. Value might change once discard or commit is called.
// Use ValueCopy if you want to do a Set after Get.
func (item *Item) Value() ([]byte, error) {
	return item.val, nil
}

// ValueSize returns the size of the value without the cost of retrieving the value.
func (item *Item) ValueSize() int {
	return len(item.val)
}

// ValueCopy returns a copy of the value of the item from the value log, writing it to dst slice.
// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
// returned. Tip: It might make sense to reuse the returned slice as dst argument for the next call.
//
// This function is useful in long running iterate/update transactions to avoid a write deadlock.
// See Github issue: https://github.com/pingcap/badger/issues/315
func (item *Item) ValueCopy(dst []byte) ([]byte, error) {
	buf, err := item.Value()
	if err != nil {
		return nil, err
	}
	return y.SafeCopy(dst, buf), nil
}

func (item *Item) hasValue() bool {
	if item.meta == 0 && item.val == nil {
		// key not found
		return false
	}
	return true
}

// IsDeleted returns true if item contains deleted or expired value.
func (item *Item) IsDeleted() bool {
	return table.IsDeleted(item.meta)
}

// EstimatedSize returns approximate size of the key-value pair.
//
// This can be called while iterating through a store to quickly estimate the
// size of a range of key-value pairs (without fetching the corresponding
// values).
func (item *Item) EstimatedSize() int64 {
	if !item.hasValue() {
		return 0
	}
	return int64(len(item.key) + len(item.val))
}

// UserMeta returns the userMeta set by the user. Typically, this byte, optionally set by the user
// is used to interpret the value.
func (item *Item) UserMeta() []byte {
	return item.userMeta
}

// Iterator helps iterating over the KV pairs in a lexicographically sorted order.
type Iterator struct {
	iitr        table.Iterator
	readTs      uint64
	reversed    bool
	allVersions bool

	item  *Item
	itBuf Item
	vs    y.ValueStruct
	bound []byte

	closed bool
}

// Item returns pointer to the current key-value pair.
// This item is only valid until it.Next() gets called.
func (it *Iterator) Item() *Item {
	return it.item
}

// Valid returns false when iteration is done.
func (it *Iterator) Valid() bool { return it.item != nil }

// ValidForPrefix returns false when iteration is done
// or when the current key is not prefixed by the specified prefix.
func (it *Iterator) ValidForPrefix(prefix []byte) bool {
	return it.item != nil && bytes.HasPrefix(it.item.key, prefix)
}

// Close would close the iterator. It is important to call this when you're done with iteration.
func (it *Iterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	it.iitr.Close()
}

// Next would advance the iterator by one. Always check it.Valid() after a Next()
// to ensure you have access to a valid it.Item().
func (it *Iterator) Next() {
	if it.allVersions && it.Valid() && it.iitr.NextVersion() {
		it.updateItem()
		return
	}
	it.iitr.Next()
	it.parseItem()
	return
}

func (it *Iterator) updateItem() {
	it.iitr.FillValue(&it.vs)
	item := &it.itBuf
	item.key = it.iitr.Key()
	item.meta = it.vs.Meta
	item.userMeta = it.vs.UserMeta
	item.val = it.vs.Value
	it.item = item
}

func (it *Iterator) parseItem() {
	iitr := it.iitr
	for iitr.Valid() {
		key := iitr.Key()
		if iitr.Value().Version > it.readTs {
			if !table.SeekToVersion(iitr, it.readTs) {
				iitr.Next()
				continue
			}
		}
		it.updateItem()
		if !it.allVersions && table.IsDeleted(it.vs.Meta) {
			if it.reversed {
				if bytes.Compare(key, it.bound) < 0 {
					break
				}
			} else {
				if bytes.Compare(key, it.bound) >= 0 {
					break
				}
			}
			iitr.Next()
			continue
		}
		return
	}
	it.item = nil
}

// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
// greater than provided if iterating in the forward direction. Behavior would be reversed is
// iterating backwards.
func (it *Iterator) Seek(key []byte) {
	if !it.reversed {
		it.iitr.Seek(key)
	} else {
		if len(key) == 0 {
			it.iitr.Rewind()
		} else {
			it.iitr.Seek(key)
		}
	}
	it.parseItem()
}

// Rewind would rewind the iterator cursor all the way to zero-th position, which would be the
// smallest key if iterating forward, and largest if iterating backward. It does not keep track of
// whether the cursor started with a Seek().
func (it *Iterator) Rewind() {
	it.iitr.Rewind()
	it.parseItem()
}

func (it *Iterator) SetAllVersions(allVersions bool) {
	it.allVersions = allVersions
}

func (s *Snapshot) NewIterator(cf int, reversed, allVersions bool) *Iterator {
	iter := &Iterator{
		iitr:        s.newIterator(cf, reversed),
		reversed:    reversed,
		allVersions: allVersions,
	}
	if s.cfs[cf].Managed && s.managedReadTS != 0 {
		iter.readTs = s.managedReadTS
	} else {
		iter.readTs = math.MaxUint64
	}
	return iter
}

func (s *Snapshot) newIterator(cf int, reverse bool) table.Iterator {
	iters := make([]table.Iterator, 0, 12)
	if s.shard.isSplitting() {
		for i := 0; i < len(s.shard.splittingMemTbls); i++ {
			memTbl := s.shard.loadSplittingMemTable(i)
			it := memTbl.NewIterator(cf, reverse)
			if it != nil {
				iters = append(iters, it)
			}
		}
	}
	memTbls := s.shard.loadMemTables()
	iters = s.appendMemTblIters(iters, memTbls, cf, reverse)
	l0s := s.shard.loadL0Tables()
	iters = s.appendL0Iters(iters, l0s, cf, reverse)
	scf := s.shard.cfs[cf]
	for i := 1; i <= len(scf.levels); i++ {
		h := scf.getLevelHandler(i)
		if len(h.tables) == 0 {
			continue
		}
		iters = append(iters, table.NewConcatIterator(h.tables, reverse))
	}
	return table.NewMergeIterator(iters, reverse)
}

func (s *Snapshot) appendMemTblIters(iters []table.Iterator, memTbls *memTables, cf int, reverse bool) []table.Iterator {
	for _, tbl := range memTbls.tables {
		it := tbl.NewIterator(cf, reverse)
		if it != nil {
			iters = append(iters, it)
		}
	}
	return iters
}

func (s *Snapshot) appendL0Iters(iters []table.Iterator, l0s *l0Tables, cf int, reverse bool) []table.Iterator {
	for _, tbl := range l0s.tables {
		it := tbl.NewIterator(cf, reverse)
		if it != nil {
			iters = append(iters, it)
		}
	}
	return iters
}
