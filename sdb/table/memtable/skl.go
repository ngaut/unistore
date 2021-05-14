/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Adapted from RocksDB inline skiplist.

Key differences:
- No optimization for sequential inserts (no "prev").
- No custom comparator.
- Support overwrites. This requires care when we see the same key when inserting.
  For RocksDB or LevelDB, overwrites are implemented as a newer sequence number in the key, so
	there is no need for values. We don't intend to support versioning. In-place updates of values
	would be more efficient.
- We discard all non-concurrent code.
- We do not support Splices. This simplifies the code a lot.
- No AllocateNode or other pointer arithmetic.
- We combine the findLessThan, findGreaterOrEqual, etc into one function.
*/

package memtable

import (
	"bytes"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/badger/y"
)

const (
	maxHeight      = 14
	heightIncrease = math.MaxUint32 / 4
)

// MaxNodeSize is the memory footprint of a node of maximum height.
const (
	MaxNodeSize      = int(unsafe.Sizeof(node{}))
	EstimateNodeSize = MaxNodeSize + nodeAlign
)

type node struct {
	addr arenaAddr
	// Multiple parts of the value are encoded as a single uint64 so that it
	// can be atomically loaded and stored:
	//   value offset: uint32 (bits 0-31)
	//   value size  : uint16 (bits 32-47)
	valueAddr uint64

	// A byte slice is 24 bytes. We are trying to save space here.
	keyAddr arenaAddr // Immutable. No need to lock to access key.

	// Height of the tower.
	height uint16

	// Most nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated in the arena, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	//
	// All accesses to elements should use CAS operations, with no need to lock.
	tower [maxHeight]uint64
}

// skiplist maps keys to values (in memory)
type skiplist struct {
	height int32 // Current height. 1 <= height <= kMaxHeight. CAS.
	head   *node
	arena  *arena
	randX  uint32
}

const randSeed = 410958445

// DecrRef decrements the refcount, deallocating the Skiplist when done using it
func (s *skiplist) Delete() {
	// Indicate we are closed. Good for testing.  Also, lets GC reclaim memory. Race condition
	// here would suggest we are accessing skiplist when we are supposed to have no reference!
	s.arena = nil
	s.head = nil
}

func (s *skiplist) valid() bool { return s.arena != nil }

func newNode(a *arena, key []byte, v y.ValueStruct, height int) *node {
	// The base level is already allocated in the node struct.
	offset := a.putNode(height)
	node := a.getNode(offset)
	node.addr = offset
	node.keyAddr = a.putKey(key)
	node.height = uint16(height)
	node.valueAddr = uint64(a.putVal(v))
	return node
}

// newSkiplist makes a new empty skiplist, with a given arena size
func newSkiplist(arenaSize int64) *skiplist {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, y.ValueStruct{}, maxHeight)
	return &skiplist{
		height: 1,
		head:   head,
		arena:  arena,
		randX:  randSeed,
	}
}

func (n *node) getValueAddr() arenaAddr {
	return arenaAddr(atomic.LoadUint64(&n.valueAddr))
}

func (n *node) key(arena *arena) []byte {
	return arena.getKey(n.keyAddr)
}

func (n *node) setValue(arena *arena, v y.ValueStruct) {
	for {
		oldValueAddr := atomic.LoadUint64(&n.valueAddr)
		oldValOff := n.getValueAddr()
		if oldValOff.isValueNodeAddr() {
			vn := arena.getValueNode(oldValOff)
			oldValOff = arenaAddr(vn.valAddr)
		}
		oldV := arena.getVal(oldValOff)
		if v.Version <= oldV.Version {
			// Only happens in Restore backup, do nothing.
			return
		}
		newValueAddr := arena.putVal(v)
		vn := valueNode{
			valAddr:     uint64(newValueAddr),
			nextValAddr: oldValueAddr,
		}
		valueNodeAddr := arena.putValueNode(vn)
		if !atomic.CompareAndSwapUint64(&n.valueAddr, oldValueAddr, uint64(valueNodeAddr)) {
			continue
		}
		break
	}
}

func (n *node) getNextOffset(h int) arenaAddr {
	return arenaAddr(atomic.LoadUint64(&n.tower[h]))
}

func (n *node) casNextOffset(h int, old, val arenaAddr) bool {
	return atomic.CompareAndSwapUint64(&n.tower[h], uint64(old), uint64(val))
}

// Returns true if key is strictly > n.key.
// If n is nil, this is an "end" marker and we return false.
//func (s *Skiplist) keyIsAfterNode(key []byte, n *node) bool {
//	y.Assert(n != s.head)
//	return n != nil && y.CompareKeysWithVer(key, n.key) > 0
//}

func (s *skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && s.nextRand() <= heightIncrease {
		h++
	}
	return h
}

func (s *skiplist) nextRand() uint32 {
	// See https://en.wikipedia.org/wiki/Xorshift
	x := s.randX
	x ^= x << 13
	x ^= x >> 17
	x ^= x << 5
	s.randX = x
	return x
}

func (s *skiplist) getNext(nd *node, height int) *node {
	return s.arena.getNode(nd.getNextOffset(height))
}

// findNear finds the node near to key.
// If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
// node.key <= key (if allowEqual=true).
// If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
// node.key >= key (if allowEqual=true).
// Returns the node found. The bool returned is true if the node has key equal to given key.

func (s *skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := s.head
	level := int(s.getHeight() - 1)
	var afterNode *node
	for {
		// Assume x.key < key.
		next := s.getNext(x, level)
		if next == nil {
			// x.key < key < END OF LIST
			if level > 0 {
				// Can descend further to iterate closer to the end.
				level--
				continue
			}
			// Level=0. Cannot descend further. Let's return something that makes sense.
			if !less {
				return nil, false
			}
			// Try to return x. Make sure it is not a head node.
			if x == s.head {
				return nil, false
			}
			return x, false
		}
		var cmp int
		if next == afterNode {
			// We compared the same node on the upper level, no need to compare again.
			cmp = -1
		} else {
			nextKey := next.key(s.arena)
			cmp = bytes.Compare(key, nextKey)
		}
		if cmp > 0 {
			// x.key < next.key < key. We can continue to move right.
			x = next
			continue
		}
		if cmp == 0 {
			// x.key < key == next.key.
			if allowEqual {
				return next, true
			}
			if !less {
				// We want >, so go to base level to grab the next bigger note.
				return s.getNext(next, 0), false
			}
			// We want <. If not base level, we should go closer in the next level.
			if level > 0 {
				level--
				continue
			}
			// On base level. Return x.
			if x == s.head {
				return nil, false
			}
			return x, false
		}
		// cmp < 0. In other words, x.key < key < next.
		if level > 0 {
			afterNode = next
			level--
			continue
		}
		// At base level. Need to return something.
		if !less {
			return next, false
		}
		// Try to return x. Make sure it is not a head node.
		if x == s.head {
			return nil, false
		}
		return x, false
	}
}

// findSpliceForLevel returns (outBefore, outAfter, match) with outBefore.key < key <= outAfter.key.
// The input "before" tells us where to start looking.
// If we found a node with the same key, then we return match = true.
// Otherwise, outBefore.key < key < outAfter.key.
func (s *skiplist) findSpliceForLevel(key []byte, before *node, level int) (*node, *node, bool) {
	for {
		// Assume before.key < key.
		next := s.getNext(before, level)
		if next == nil {
			return before, next, false
		}
		nextKey := next.key(s.arena)
		cmp := bytes.Compare(key, nextKey)
		if cmp <= 0 {
			return before, next, cmp == 0
		}
		before = next // Keep moving right on this level.
	}
}

func (s *skiplist) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// Put inserts the key-value pair.
func (s *skiplist) Put(key []byte, v y.ValueStruct) {
	s.PutWithHint(key, v, nil)
}

// Hint is used to speed up sequential write.
type hint struct {
	height int32

	// hitHeight is used to reduce cost of calculateRecomputeHeight.
	// For random workload, comparing hint keys from bottom up is wasted work.
	// So we record the hit height of the last operation, only grow recompute height from near that height.
	hitHeight int32
	prev      [maxHeight + 1]*node
	next      [maxHeight + 1]*node
}

func (s *skiplist) calculateRecomputeHeight(key []byte, h *hint, listHeight int32) int32 {
	if h.height < listHeight {
		// Either splice is never used or list height has grown, we recompute all.
		h.prev[listHeight] = s.head
		h.next[listHeight] = nil
		h.height = int32(listHeight)
		h.hitHeight = h.height
		return listHeight
	}
	recomputeHeight := h.hitHeight - 2
	if recomputeHeight < 0 {
		recomputeHeight = 0
	}
	for recomputeHeight < listHeight {
		prevNode := h.prev[recomputeHeight]
		nextNode := h.next[recomputeHeight]
		prevNext := s.getNext(prevNode, int(recomputeHeight))
		if prevNext != nextNode {
			recomputeHeight++
			continue
		}
		if prevNode != s.head &&
			prevNode != nil &&
			bytes.Compare(key, prevNode.key(s.arena)) <= 0 {
			// Key is before splice.
			for prevNode == h.prev[recomputeHeight] {
				recomputeHeight++
			}
			continue
		}
		if nextNode != nil && bytes.Compare(key, nextNode.key(s.arena)) > 0 {
			// Key is after splice.
			for nextNode == h.next[recomputeHeight] {
				recomputeHeight++
			}
			continue
		}
		break
	}
	h.hitHeight = recomputeHeight
	return recomputeHeight
}

// PutWithHint inserts the key-value pair with Hint for better sequential write performance.
func (s *skiplist) PutWithHint(key []byte, v y.ValueStruct, h *hint) {
	// Since we allow overwrite, we may not need to create a new node. We might not even need to
	// increase the height. Let's defer these actions.
	listHeight := s.getHeight()
	height := s.randomHeight()

	// Try to increase s.height via CAS.
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			// Successfully increased skiplist.height.
			listHeight = int32(height)
			break
		}
		listHeight = s.getHeight()
	}
	spliceIsValid := h != nil
	if h == nil {
		h = new(hint)
	}
	recomputeHeight := s.calculateRecomputeHeight(key, h, listHeight)
	if recomputeHeight > 0 {
		for i := recomputeHeight - 1; i >= 0; i-- {
			var match bool
			h.prev[i], h.next[i], match = s.findSpliceForLevel(key, h.prev[i+1], int(i))
			if match {
				// In place update.
				h.next[i].setValue(s.arena, v)
				for i > 0 {
					h.prev[i-1] = h.prev[i]
					h.next[i-1] = h.next[i]
					i--
				}
				return
			}
		}
	} else {
		// Even the recomputeHeight is 0, we still need to check match and do in place update to insert the new version.
		if h.next[0] != nil && bytes.Equal(h.next[0].key(s.arena), key) {
			h.next[0].setValue(s.arena, v)
			return
		}
	}

	// We do need to create a new node.
	x := newNode(s.arena, key, v, height)

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := 0; i < height; i++ {
		for {
			nextOffset := s.arena.getNodeOffset(h.next[i])
			x.tower[i] = uint64(nextOffset)
			if h.prev[i].casNextOffset(i, nextOffset, s.arena.getNodeOffset(x)) {
				// Managed to insert x between prev[i] and next[i]. Go to the next level.
				break
			}
			// CAS failed. We need to recompute prev and next.
			// It is unlikely to be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
			h.prev[i], h.next[i], _ = s.findSpliceForLevel(key, h.prev[i], i)
			if i > 0 {
				spliceIsValid = false
			}
		}
	}
	if spliceIsValid {
		for i := 0; i < height; i++ {
			h.prev[i] = x
			h.next[i] = s.getNext(x, i)
		}
	} else {
		h.height = 0
	}
}

func (s *skiplist) GetWithHint(key []byte, version uint64, h *hint) y.ValueStruct {
	if h == nil {
		h = new(hint)
	}
	listHeight := s.getHeight()
	recomputeHeight := s.calculateRecomputeHeight(key, h, listHeight)
	var n *node
	if recomputeHeight > 0 {
		for i := recomputeHeight - 1; i >= 0; i-- {
			var match bool
			h.prev[i], h.next[i], match = s.findSpliceForLevel(key, h.prev[i+1], int(i))
			if match {
				n = h.next[i]
				for j := i; j >= 0; j-- {
					h.prev[j] = n
					h.next[j] = s.getNext(n, int(j))
				}
				break
			}
		}
	} else {
		n = h.next[0]
	}
	if n == nil {
		return y.ValueStruct{}
	}
	nextKey := s.arena.getKey(n.keyAddr)
	if !bytes.Equal(key, nextKey) {
		return y.ValueStruct{}
	}
	valOffset := n.getValueAddr()
	var v y.ValueStruct
	for valOffset.isValueNodeAddr() {
		vn := s.arena.getValueNode(valOffset)
		s.arena.fillVal(&v, arenaAddr(vn.valAddr))
		if v.Version <= version {
			return v
		}
		if vn.nextValAddr == 0 {
			return y.ValueStruct{}
		}
		valOffset = arenaAddr(vn.nextValAddr)
	}
	vs := s.arena.getVal(valOffset)
	return vs
}

// Empty returns if the Skiplist is empty.
func (s *skiplist) Empty() bool {
	return s.findLast() == nil
}

// findLast returns the last element. If head (empty list), we return nil. All the find functions
// will NEVER return the head nodes.
func (s *skiplist) findLast() *node {
	n := s.head
	level := int(s.getHeight()) - 1
	for {
		next := s.getNext(n, level)
		if next != nil {
			n = next
			continue
		}
		if level == 0 {
			if n == s.head {
				return nil
			}
			return n
		}
		level--
	}
}

// Get gets the value associated with the key. It returns a valid value if it finds equal or earlier
// version of the same key.
func (s *skiplist) Get(key []byte, version uint64) y.ValueStruct {
	n, _ := s.findNear(key, false, true) // findGreaterOrEqual.
	if n == nil {
		return y.ValueStruct{}
	}

	nextKey := s.arena.getKey(n.keyAddr)
	if !bytes.Equal(key, nextKey) {
		return y.ValueStruct{}
	}
	valOffset := n.getValueAddr()
	var v y.ValueStruct
	for valOffset.isValueNodeAddr() {
		vn := s.arena.getValueNode(valOffset)
		s.arena.fillVal(&v, arenaAddr(vn.valAddr))
		if version >= v.Version {
			return v
		}
		valOffset = arenaAddr(vn.nextValAddr)
	}
	s.arena.fillVal(&v, valOffset)
	if version >= v.Version {
		return v
	}
	return y.ValueStruct{}
}

// The DeleteKey operation is not thread safe, it can only be run on a single thread.
func (s *skiplist) DeleteKey(key []byte) bool {
	h := new(hint)
	listHeight := s.getHeight()
	recomputeHeight := s.calculateRecomputeHeight(key, h, listHeight)
	var n *node
	if recomputeHeight > 0 {
		for i := recomputeHeight - 1; i >= 0; i-- {
			var match bool
			h.prev[i], h.next[i], match = s.findSpliceForLevel(key, h.prev[i+1], int(i))
			if match {
				n = h.next[i]
			}
		}
	} else {
		n = h.next[0]
	}
	if n == nil {
		return false
	}
	nodeOff := s.arena.getNodeOffset(n)
	for i := int(n.height) - 1; i >= 0; i-- {
		// Change the nexts from higher to lower, so the data is consistent at any point.
		y.Assert(h.prev[i].casNextOffset(i, nodeOff, n.getNextOffset(i)))
	}
	return true
}

// NewIterator returns a skiplist iterator.  You have to Close() the iterator.
func (s *skiplist) NewIterator() *Iterator {
	return &Iterator{list: s}
}

// MemSize returns the size of the Skiplist in terms of how much memory is used within its internal
// arena.
func (s *skiplist) MemSize() int64 { return s.arena.size() }

// Iterator is an iterator over skiplist object. For new objects, you just
// need to initialize Iterator.list.
type Iterator struct {
	list *skiplist
	n    *node

	uk         []byte
	v          y.ValueStruct
	valList    []uint64
	valListIdx int
}

// Valid returns true iff the iterator is positioned at a valid node.
func (s *Iterator) Valid() bool { return s.n != nil }

// Key returns the key at the current position.
func (s *Iterator) Key() y.Key {
	return y.KeyWithTs(s.uk, s.v.Version)
}

// Value returns value.
func (s *Iterator) Value() y.ValueStruct {
	return s.v
}

// FillValue fills value.
func (s *Iterator) FillValue(vs *y.ValueStruct) {
	*vs = s.v
}

// Next advances to the next position.
func (s *Iterator) Next() {
	y.Assert(s.Valid())
	s.n = s.list.getNext(s.n, 0)
	s.loadNode()
}

func (s *Iterator) NextVersion() bool {
	if s.valListIdx+1 < len(s.valList) {
		s.setValueListIdx(s.valListIdx + 1)
		return true
	}
	return false
}

// Prev advances to the previous position.
func (s *Iterator) Prev() {
	y.Assert(s.Valid())
	s.n, _ = s.list.findNear(s.uk, true, false) // find <. No equality allowed.
	s.loadNode()
}

// Seek advances to the first entry with a key >= target.
func (s *Iterator) Seek(target []byte) {
	s.n, _ = s.list.findNear(target, false, true) // find >=.
	s.loadNode()
}

func (s *Iterator) loadNode() {
	if s.n == nil {
		return
	}
	if len(s.valList) > 0 {
		s.valList = s.valList[:0]
		s.valListIdx = 0
	}
	s.uk = s.n.key(s.list.arena)
	off := s.n.getValueAddr()
	if !off.isValueNodeAddr() {
		s.list.arena.fillVal(&s.v, off)
		return
	}
	for {
		vn := s.list.arena.getValueNode(off)
		s.valList = append(s.valList, vn.valAddr)
		off = arenaAddr(vn.nextValAddr)
		if !off.isValueNodeAddr() {
			s.valList = append(s.valList, vn.nextValAddr)
			break
		}
	}
	s.setValueListIdx(0)
}

func (s *Iterator) setValueListIdx(idx int) {
	s.valListIdx = idx
	off := arenaAddr(s.valList[idx])
	s.list.arena.fillVal(&s.v, off)
}

// SeekForPrev finds an entry with key <= target.
func (s *Iterator) SeekForPrev(target []byte) {
	s.n, _ = s.list.findNear(target, true, true) // find <=.
	s.loadNode()
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToFirst() {
	s.n = s.list.getNext(s.list.head, 0)
	s.loadNode()
}

// SeekToLast seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *Iterator) SeekToLast() {
	s.n = s.list.findLast()
	s.loadNode()
}

func (s *Iterator) Close() error {
	return nil
}

// UniIterator is a unidirectional memtable iterator. It is a thin wrapper around
// Iterator. We like to keep Iterator as before, because it is more powerful and
// we might support bidirectional iterators in the future.
type UniIterator struct {
	iter     *Iterator
	reversed bool
}

// NewUniIterator returns a UniIterator.
func (s *skiplist) NewUniIterator(reversed bool) *UniIterator {
	return &UniIterator{
		iter:     s.NewIterator(),
		reversed: reversed,
	}
}

// Next implements y.Interface
func (s *UniIterator) Next() {
	if !s.reversed {
		s.iter.Next()
	} else {
		s.iter.Prev()
	}
}

func (s *UniIterator) NextVersion() bool {
	return s.iter.NextVersion()
}

// Rewind implements y.Interface
func (s *UniIterator) Rewind() {
	if !s.reversed {
		s.iter.SeekToFirst()
	} else {
		s.iter.SeekToLast()
	}
}

// Seek implements y.Interface
func (s *UniIterator) Seek(key []byte) {
	if !s.reversed {
		s.iter.Seek(key)
	} else {
		s.iter.SeekForPrev(key)
	}
}

// Key implements y.Interface
func (s *UniIterator) Key() y.Key { return s.iter.Key() }

// Value implements y.Interface
func (s *UniIterator) Value() y.ValueStruct { return s.iter.Value() }

// FillValue implements y.Interface
func (s *UniIterator) FillValue(vs *y.ValueStruct) { s.iter.FillValue(vs) }

// Valid implements y.Interface
func (s *UniIterator) Valid() bool { return s.iter.Valid() }

func (s *UniIterator) Close() error { return s.iter.Close() }
