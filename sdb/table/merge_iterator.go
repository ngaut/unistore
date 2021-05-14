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
	second  y.Iterator
	reverse bool
	sameKey bool
}

type mergeIteratorChild struct {
	valid bool
	key   y.Key
	iter  y.Iterator

	// The two iterators are type asserted from `y.Iterator`, used to inline more function calls.
	merge  *MergeIterator
	concat *ConcatIterator
}

func (child *mergeIteratorChild) setIterator(iter y.Iterator) {
	child.iter = iter
	child.merge, _ = iter.(*MergeIterator)
	child.concat, _ = iter.(*ConcatIterator)
}

func (child *mergeIteratorChild) reset() {
	if child.merge != nil {
		child.valid = child.merge.smaller.valid
		if child.valid {
			child.key = child.merge.smaller.key
		}
	} else if child.concat != nil {
		child.valid = child.concat.Valid()
		if child.valid {
			child.key = child.concat.Key()
		}
	} else {
		child.valid = child.iter.Valid()
		if child.valid {
			child.key = child.iter.Key()
		}
	}
}

func (mt *MergeIterator) fix() {
	if !mt.bigger.valid {
		return
	}
	for mt.smaller.valid {
		cmp := bytes.Compare(mt.smaller.key.UserKey, mt.bigger.key.UserKey)
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
	if mt.smaller.key.Version < mt.bigger.key.Version {
		// The smaller is second iterator, the bigger is first iterator.
		// We have swapped already, no more versions.
		return false
	}
	if mt.smaller.key.Version == mt.bigger.key.Version {
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
func (mt *MergeIterator) Key() y.Key {
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
func NewMergeIterator(iters []y.Iterator, reverse bool) y.Iterator {
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
	return NewMergeIterator([]y.Iterator{NewMergeIterator(iters[:mid], reverse), NewMergeIterator(iters[mid:], reverse)}, reverse)
}

type EmptyIterator struct{}

func (e *EmptyIterator) Next() {}

func (e *EmptyIterator) NextVersion() bool {
	return false
}

func (e *EmptyIterator) Rewind() {}

func (e *EmptyIterator) Seek(key []byte) {}

func (e *EmptyIterator) Key() y.Key {
	return y.Key{}
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
