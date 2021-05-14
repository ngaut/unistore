package table

import (
	"bytes"
	"sort"

	"github.com/pingcap/badger/y"
)

// ConcatIterator concatenates the sequences defined by several iterators.  (It only works with
// TableIterators, probably just because it's faster to not be so generic.)
type ConcatIterator struct {
	idx      int // Which iterator is active now.
	cur      y.Iterator
	iters    []y.Iterator // Corresponds to tables.
	tables   []Table      // Disregarding reversed, this is in ascending order.
	reversed bool
}

// NewConcatIterator creates a new concatenated iterator
func NewConcatIterator(tbls []Table, reversed bool) *ConcatIterator {
	return &ConcatIterator{
		reversed: reversed,
		iters:    make([]y.Iterator, len(tbls)),
		tables:   tbls,
		idx:      -1, // Not really necessary because s.it.Valid()=false, but good to have.
	}
}

func (s *ConcatIterator) setIdx(idx int) {
	s.idx = idx
	if idx < 0 || idx >= len(s.iters) {
		s.cur = nil
	} else {
		if s.iters[s.idx] == nil {
			ti := s.tables[s.idx].NewIterator(s.reversed)
			ti.Rewind()
			s.iters[s.idx] = ti
		}
		s.cur = s.iters[s.idx]
	}
}

// Rewind implements y.Interface
func (s *ConcatIterator) Rewind() {
	if len(s.iters) == 0 {
		return
	}
	if !s.reversed {
		s.setIdx(0)
	} else {
		s.setIdx(len(s.iters) - 1)
	}
	s.cur.Rewind()
}

// Valid implements y.Interface
func (s *ConcatIterator) Valid() bool {
	return s.cur != nil && s.cur.Valid()
}

// Key implements y.Interface
func (s *ConcatIterator) Key() y.Key {
	return s.cur.Key()
}

// Value implements y.Interface
func (s *ConcatIterator) Value() y.ValueStruct {
	return s.cur.Value()
}

func (s *ConcatIterator) FillValue(vs *y.ValueStruct) {
	s.cur.FillValue(vs)
}

// Seek brings us to element >= key if reversed is false. Otherwise, <= key.
func (s *ConcatIterator) Seek(key []byte) {
	var idx int
	if !s.reversed {
		idx = sort.Search(len(s.tables), func(i int) bool {
			return bytes.Compare(s.tables[i].Biggest().UserKey, key) >= 0
		})
	} else {
		n := len(s.tables)
		idx = n - 1 - sort.Search(n, func(i int) bool {
			return bytes.Compare(s.tables[n-1-i].Smallest().UserKey, key) <= 0
		})
	}
	if idx >= len(s.tables) || idx < 0 {
		s.setIdx(-1)
		return
	}
	// For reversed=false, we know s.tables[i-1].Biggest() < key. Thus, the
	// previous table cannot possibly contain key.
	s.setIdx(idx)
	s.cur.Seek(key)
}

// Next advances our concat iterator.
func (s *ConcatIterator) Next() {
	s.cur.Next()
	if s.cur.Valid() {
		// Nothing to do. Just stay with the current table.
		return
	}
	for { // In case there are empty tables.
		if !s.reversed {
			s.setIdx(s.idx + 1)
		} else {
			s.setIdx(s.idx - 1)
		}
		if s.cur == nil {
			// End of list. Valid will become false.
			return
		}
		s.cur.Rewind()
		if s.cur.Valid() {
			break
		}
	}
}

func (s *ConcatIterator) NextVersion() bool {
	return s.cur.NextVersion()
}

// Close implements y.Interface.
func (s *ConcatIterator) Close() error {
	for _, it := range s.iters {
		if it == nil {
			continue
		}
		if err := it.Close(); err != nil {
			return y.Wrapf(err, "ConcatIterator")
		}
	}
	return nil
}
