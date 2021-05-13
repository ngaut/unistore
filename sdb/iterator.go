package sdb

import (
	"bytes"
	"github.com/pingcap/badger/table"
	"github.com/pingcap/badger/y"
	"math"
)

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
	item.vptr = it.vs.Value
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
		iitr: s.newShardIterator(cf, reversed, 0),
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

func (s *Snapshot) newShardIterator(cf int, reverse bool, sinceCommitTS uint64) y.Iterator {
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
	iters = s.appendL0Iters(iters, l0s, cf, reverse, sinceCommitTS)
	if sinceCommitTS > 0 {
		// This is a delta iterator, we don't read data >= L1.
		return table.NewMergeIterator(iters, reverse)
	}
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

func (s *Snapshot) appendMemTblIters(iters []y.Iterator, memTbls *shardingMemTables, cf int, reverse bool) []y.Iterator {
	for _, tbl := range memTbls.tables {
		it := tbl.NewIterator(cf, reverse)
		if it != nil {
			iters = append(iters, it)
		}
	}
	return iters
}

func (s *Snapshot) appendL0Iters(iters []y.Iterator, l0s *shardL0Tables, cf int, reverse bool, sinceCommitTS uint64) []y.Iterator {
	for _, tbl := range l0s.tables {
		if tbl.commitTS <= sinceCommitTS {
			continue
		}
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
