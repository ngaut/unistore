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

package memtable

import (
	"encoding/binary"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/badger/y"
)

const (
	offsetSize = int(unsafe.Sizeof(arenaAddr(0)))

	// Always align nodes on 64-bit boundaries, even on 32-bit architectures,
	// so that the node.value field is 64-bit aligned. This is necessary because
	// node.getValueAddr uses atomic.LoadUint64, which expects its input
	// pointer to be 64-bit aligned.
	nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1
)

// Arena should be lock-free.
type arena struct {
	arenaPtr unsafe.Pointer
}

// newArena returns a new arena.
func newArena() *arena {
	// Don't store data at position 0 in order to reserve offset=0 as a kind
	// of nil pointer.
	out := &arena{
		arenaPtr: unsafe.Pointer(newArenaLocator()),
	}
	return out
}

func (s *arena) getArenaLocator() *arenaLocator {
	return (*arenaLocator)(atomic.LoadPointer(&s.arenaPtr))
}

func (s *arena) setArenaLocator(al *arenaLocator) {
	atomic.StorePointer(&s.arenaPtr, unsafe.Pointer(al))
}

func (s *arena) alloc(size int) arenaAddr {
	al := s.getArenaLocator()
	addr := al.alloc(size)
	if addr == nullArenaAddr {
		al = al.grow()
		s.setArenaLocator(al)
		// The new arena block must have enough memory to alloc.
		addr = al.alloc(size)
	}
	return addr
}

func (s *arena) get(addr arenaAddr) []byte {
	al := s.getArenaLocator()
	return al.blocks[addr.blockIdx()].get(addr.blockOffset(), addr.size())
}

func (s *arena) size() int64 {
	al := s.getArenaLocator()
	return int64((len(al.blocks)-1)*al.blockSize) + int64(atomic.LoadUint32(&al.blocks[len(al.blocks)-1].length))
}

// putNode allocates a node in the arena. The node is aligned on a pointer-sized
// boundary. The arena offset of the node is returned.
func (s *arena) putNode(height int) arenaAddr {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - height) * offsetSize

	nodeSize := uint32(MaxNodeSize - unusedSize)
	return s.alloc(int(nodeSize))
}

// Put will *copy* val into arena. To make better use of this, reuse your input
// val buffer. Returns an offset into buf. User is responsible for remembering
// size of val. We could also store this size inside arena but the encoding and
// decoding will incur some overhead.
func (s *arena) putVal(v y.ValueStruct) arenaAddr {
	size := v.EncodedSize()
	addr := s.alloc(int(size))
	v.Encode(s.get(addr))
	return addr
}

func (s *arena) putKey(key []byte) arenaAddr {
	size := len(key)
	addr := s.alloc(size)
	copy(s.get(addr), key)
	return addr
}

// getNode returns a pointer to the node located at offset. If the offset is
// zero, then the nil node pointer is returned.
func (s *arena) getNode(offset arenaAddr) *node {
	if offset == 0 {
		return nil
	}
	data := s.get(offset)
	return (*node)(unsafe.Pointer(&data[0]))
}

// getKey returns byte slice at offset.
func (s *arena) getKey(addr arenaAddr) (k []byte) {
	return s.get(addr)
}

// getVal returns byte slice at offset. The given size should be just the value
// size and should NOT include the meta bytes.
func (s *arena) getVal(addr arenaAddr) (ret y.ValueStruct) {
	ret.Decode(s.get(addr))
	return
}

func (s *arena) fillVal(vs *y.ValueStruct, addr arenaAddr) {
	vs.Decode(s.get(addr))
}

// getNodeOffset returns the offset of node in the arena. If the node pointer is
// nil, then the zero offset is returned.
func (s *arena) getNodeOffset(nd *node) arenaAddr {
	if nd == nil {
		return 0
	}
	return nd.addr
}

const valueNodeSize = uint32(unsafe.Sizeof(valueNode{}))

// valueNode is used to store multiple versions of a key.
// We enforce that the newly put value must have a greater version, so the single linked list is in descending order
// by version.
type valueNode struct {
	valAddr     uint64
	nextValAddr uint64
}

func (vn valueNode) encode(b []byte) {
	binary.LittleEndian.PutUint64(b, vn.valAddr)
	binary.LittleEndian.PutUint64(b[8:], vn.nextValAddr)
}

func (vn *valueNode) decode(b []byte) {
	vn.valAddr = binary.LittleEndian.Uint64(b)
	vn.nextValAddr = binary.LittleEndian.Uint64(b[8:])
}

func (s *arena) putValueNode(vn valueNode) arenaAddr {
	addr := s.alloc(int(valueNodeSize))
	vn.encode(s.get(addr))
	addr.markValueNodeAddr()
	return addr
}

func (s *arena) getValueNode(addr arenaAddr) valueNode {
	var vl valueNode
	vl.decode(s.get(addr))
	return vl
}
