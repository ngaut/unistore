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
	key      y.Key
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
	return item.key.UserKey
}

// KeyCopy returns a copy of the key of the item, writing it to dst slice.
// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
// returned.
func (item *Item) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, item.key.UserKey)
}

// Version returns the commit timestamp of the item.
func (item *Item) Version() uint64 {
	return item.key.Version
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
	return isDeleted(item.meta)
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
	return int64(item.key.Len() + len(item.val))
}

// UserMeta returns the userMeta set by the user. Typically, this byte, optionally set by the user
// is used to interpret the value.
func (item *Item) UserMeta() []byte {
	return item.userMeta
}

// IteratorOptions is used to set options when iterating over Badger key-value
// stores.
//
// This package provides DefaultIteratorOptions which contains options that
// should work for most applications. Consider using that as a starting point
// before customizing it for your own needs.
type IteratorOptions struct {
	Reverse     bool // Direction of iteration. False is forward, true is backward.
	AllVersions bool // Fetch all valid versions of the same key.

	// StartKey and EndKey are used to prune non-overlapping table iterators.
	// They are not boundary limits, the EndKey is exclusive.
	StartKey y.Key
	EndKey   y.Key

	internalAccess bool // Used to allow internal access to badger keys.
}

// Iterator helps iterating over the KV pairs in a lexicographically sorted order.
type Iterator struct {
	iitr   y.Iterator
	readTs uint64

	opt   IteratorOptions
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
	return it.item != nil && bytes.HasPrefix(it.item.key.UserKey, prefix)
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
	if it.opt.AllVersions && it.Valid() && it.iitr.NextVersion() {
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
		if !it.opt.internalAccess && key.UserKey[0] == '!' {
			iitr.Next()
			continue
		}
		if key.Version > it.readTs {
			if !y.SeekToVersion(iitr, it.readTs) {
				iitr.Next()
				continue
			}
		}
		it.updateItem()
		if !it.opt.AllVersions && isDeleted(it.vs.Meta) {
			if it.opt.Reverse {
				if bytes.Compare(key.UserKey, it.bound) < 0 {
					break
				}
			} else {
				if bytes.Compare(key.UserKey, it.bound) >= 0 {
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

func isDeleted(meta byte) bool {
	return meta&bitDelete > 0
}

// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
// greater than provided if iterating in the forward direction. Behavior would be reversed is
// iterating backwards.
func (it *Iterator) Seek(key []byte) {
	if !it.opt.Reverse {
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
	it.opt.AllVersions = allVersions
}

func (s *Snapshot) NewIterator(cf int, reversed, allVersions bool) *Iterator {
	iter := &Iterator{
		iitr: s.newIterator(cf, reversed),
		opt:  IteratorOptions{Reverse: reversed, AllVersions: allVersions},
	}
	if s.cfs[cf].Managed {
		if s.managedReadTS != 0 {
			iter.readTs = s.managedReadTS
		} else {
			iter.readTs = math.MaxUint64
		}
	} else {
		iter.readTs = s.readTS
	}
	return iter
}

func (s *Snapshot) newIterator(cf int, reverse bool) y.Iterator {
	iters := make([]y.Iterator, 0, 12)
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
	for i := 1; i <= ShardMaxLevel; i++ {
		h := scf.getLevelHandler(i)
		if len(h.tables) == 0 {
			continue
		}
		iters = append(iters, table.NewConcatIterator(h.tables, reverse))
	}
	return table.NewMergeIterator(iters, reverse)
}

func (s *Snapshot) appendMemTblIters(iters []y.Iterator, memTbls *memTables, cf int, reverse bool) []y.Iterator {
	for _, tbl := range memTbls.tables {
		it := tbl.NewIterator(cf, reverse)
		if it != nil {
			iters = append(iters, it)
		}
	}
	return iters
}

func (s *Snapshot) appendL0Iters(iters []y.Iterator, l0s *l0Tables, cf int, reverse bool) []y.Iterator {
	for _, tbl := range l0s.tables {
		it := tbl.newIterator(cf, reverse)
		if it != nil {
			iters = append(iters, it)
		}
	}
	return iters
}

func appendIteratorsReversed(out []y.Iterator, th []table.Table, reversed bool) []y.Iterator {
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, table.NewConcatIterator(th[i:i+1], reversed))
	}
	return out
}
