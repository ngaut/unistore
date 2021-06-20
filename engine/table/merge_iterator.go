// Copyright 2019-present PingCAP, Inc.
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

package table

import (
	"bytes"

	"github.com/pingcap/badger/y"
)

// MergeTowIterator is a specialized MergeIterator that only merge tow iterators.
// It is an optimization for compaction.
type MergeIterator struct {
	smaller *mergeIteratorChild
	bigger  *mergeIteratorChild

	// when the two iterators has the same value, the value in the second iterator is ignored.
	second  Iterator
	reverse bool
	sameKey bool
}

type mergeIteratorChild struct {
	valid bool
	key   []byte
	ver   uint64
	iter  Iterator

	// The two iterators are type asserted from `y.Iterator`, used to inline more function calls.
	merge  *MergeIterator
	concat *ConcatIterator
}

func (child *mergeIteratorChild) setIterator(iter Iterator) {
	child.iter = iter
	child.merge, _ = iter.(*MergeIterator)
	child.concat, _ = iter.(*ConcatIterator)
}

func (child *mergeIteratorChild) reset() {
	if child.merge != nil {
		child.valid = child.merge.smaller.valid
		if child.valid {
			child.key = child.merge.smaller.key
			child.ver = child.merge.smaller.ver
		}
	} else if child.concat != nil {
		child.valid = child.concat.Valid()
		if child.valid {
			child.key = child.concat.Key()
			child.ver = child.concat.Value().Version
		}
	} else {
		child.valid = child.iter.Valid()
		if child.valid {
			child.key = child.iter.Key()
			child.ver = child.iter.Value().Version
		}
	}
}

func (mt *MergeIterator) fix() {
	if !mt.bigger.valid {
		return
	}
	for mt.smaller.valid {
		cmp := bytes.Compare(mt.smaller.key, mt.bigger.key)
		if cmp == 0 {
			mt.sameKey = true
			if mt.smaller.iter == mt.second {
				mt.swap()
			}
			return
		}
		mt.sameKey = false
		if mt.reverse {
			if cmp < 0 {
				mt.swap()
			}
		} else {
			if cmp > 0 {
				mt.swap()
			}
		}
		return
	}
	mt.swap()
}

func (mt *MergeIterator) swap() {
	mt.smaller, mt.bigger = mt.bigger, mt.smaller
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (mt *MergeIterator) Next() {
	if mt.smaller.merge != nil {
		mt.smaller.merge.Next()
	} else if mt.smaller.concat != nil {
		mt.smaller.concat.Next()
	} else {
		mt.smaller.iter.Next()
	}
	mt.smaller.reset()
	if mt.sameKey && mt.bigger.valid {
		if mt.bigger.merge != nil {
			mt.bigger.merge.Next()
		} else if mt.bigger.concat != nil {
			mt.bigger.concat.Next()
		} else {
			mt.bigger.iter.Next()
		}
		mt.bigger.reset()
	}
	mt.fix()
}

func (mt *MergeIterator) NextVersion() bool {
	if mt.smaller.iter.NextVersion() {
		mt.smaller.reset()
		return true
	}
	if !mt.sameKey {
		return false
	}
	if !mt.bigger.valid {
		return false
	}
	if mt.smaller.ver < mt.bigger.ver {
		// The smaller is second iterator, the bigger is first iterator.
		// We have swapped already, no more versions.
		return false
	}
	if mt.smaller.ver == mt.bigger.ver {
		// have duplicated key in the two iterators.
		if mt.bigger.iter.NextVersion() {
			mt.bigger.reset()
			mt.swap()
			return true
		}
		return false
	}
	mt.swap()
	return true
}

// Rewind seeks to first element (or last element for reverse iterator).
func (mt *MergeIterator) Rewind() {
	mt.smaller.iter.Rewind()
	mt.smaller.reset()
	mt.bigger.iter.Rewind()
	mt.bigger.reset()
	mt.fix()
}

// Seek brings us to element with key >= given key.
func (mt *MergeIterator) Seek(key []byte) {
	mt.smaller.iter.Seek(key)
	mt.smaller.reset()
	mt.bigger.iter.Seek(key)
	mt.bigger.reset()
	mt.fix()
}

// Valid returns whether the MergeIterator is at a valid element.
func (mt *MergeIterator) Valid() bool {
	return mt.smaller.valid
}

// Key returns the key associated with the current iterator
func (mt *MergeIterator) Key() []byte {
	return mt.smaller.key
}

// Value returns the value associated with the iterator.
func (mt *MergeIterator) Value() y.ValueStruct {
	return mt.smaller.iter.Value()
}

func (mt *MergeIterator) FillValue(vs *y.ValueStruct) {
	if mt.smaller.merge != nil {
		mt.smaller.merge.FillValue(vs)
	} else if mt.smaller.concat != nil {
		mt.smaller.concat.FillValue(vs)
	} else {
		mt.smaller.iter.FillValue(vs)
	}
}

// Close implements y.Iterator.
func (mt *MergeIterator) Close() error {
	smallerErr := mt.smaller.iter.Close()
	biggerErr := mt.bigger.iter.Close()
	if smallerErr != nil {
		return y.Wrapf(smallerErr, "MergeIterator")
	}
	if biggerErr != nil {
		return y.Wrapf(biggerErr, "MergeIterator")
	}
	return nil
}

// NewMergeIterator creates a merge iterator
func NewMergeIterator(iters []Iterator, reverse bool) Iterator {
	if len(iters) == 0 {
		return &EmptyIterator{}
	} else if len(iters) == 1 {
		return iters[0]
	} else if len(iters) == 2 {
		mi := &MergeIterator{
			second:  iters[1],
			reverse: reverse,
			smaller: new(mergeIteratorChild),
			bigger:  new(mergeIteratorChild),
		}
		mi.smaller.setIterator(iters[0])
		mi.bigger.setIterator(iters[1])
		return mi
	}
	mid := len(iters) / 2
	return NewMergeIterator([]Iterator{NewMergeIterator(iters[:mid], reverse), NewMergeIterator(iters[mid:], reverse)}, reverse)
}

type EmptyIterator struct{}

func (e *EmptyIterator) Next() {}

func (e *EmptyIterator) NextVersion() bool {
	return false
}

func (e *EmptyIterator) Rewind() {}

func (e *EmptyIterator) Seek(key []byte) {}

func (e *EmptyIterator) Key() []byte {
	return nil
}

func (e *EmptyIterator) Value() y.ValueStruct {
	return y.ValueStruct{}
}

func (e *EmptyIterator) FillValue(vs *y.ValueStruct) {}

func (e *EmptyIterator) Valid() bool {
	return false
}

func (e *EmptyIterator) Close() error {
	return nil
}
