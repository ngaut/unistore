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

package memtable

import (
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/log"
)

type arenaAddr uint64

const (
	blockAlign                = int(unsafe.Sizeof(uint64(0))) - 1
	alignMask                 = 1<<32 - int(unsafe.Sizeof(uint64(0))) // 29 bit 1 and 3 bit 0.
	nullBlockOffset           = math.MaxUint32
	nullArenaAddr   arenaAddr = 0

	valueNodeShift   uint64 = 63
	valueNodeMask    uint64 = 0x8000000000000000
	blockIdxShift    uint64 = 48
	blockIdxMask     uint64 = 0x7fff000000000000
	blockOffsetShift uint64 = 24
	blockOffsetMask  uint64 = 0x0000ffffff000000
	sizeMask         uint64 = 0x0000000000ffffff

	maxSize   = 1<<24 - 1
	blockSize = 1 << 23
)

func (addr arenaAddr) blockIdx() int {
	return int(uint64(addr)&blockIdxMask>>blockIdxShift - 1)
}

func (addr arenaAddr) blockOffset() uint32 {
	return uint32(uint64(addr) & blockOffsetMask >> blockOffsetShift)
}

func (addr arenaAddr) size() int {
	return int(uint64(addr) & sizeMask)
}

func (addr *arenaAddr) markValueNodeAddr() {
	*addr = arenaAddr(1<<valueNodeShift | uint64(*addr))
}

func (addr arenaAddr) isValueNodeAddr() bool {
	return uint64(addr)&valueNodeMask != 0
}

func newArenaAddr(blockIdx int, blockOffset uint32, size int) arenaAddr {
	return arenaAddr(uint64(blockIdx+1)<<blockIdxShift | uint64(blockOffset)<<blockOffsetShift | uint64(size))
}

type arenaLocator struct {
	blockSize int
	blocks    []*arenaBlock
}

func newArenaLocator() *arenaLocator {
	return &arenaLocator{
		blockSize: blockSize,
		blocks:    []*arenaBlock{newArenaBlock(blockSize)},
	}
}

func (a *arenaLocator) get(addr arenaAddr) []byte {
	if addr.blockIdx() >= len(a.blocks) {
		log.S().Fatalf("arena.get out of range. len(blocks)=%v, addr.blockIdx()=%v, addr.blockOffset()=%v, addr.size()=%v", len(a.blocks), addr.blockIdx(), addr.blockOffset(), addr.size())
	}
	return a.blocks[addr.blockIdx()].get(addr.blockOffset(), addr.size())
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (a *arenaLocator) alloc(size int) arenaAddr {
	if size > min(a.blockSize, maxSize) {
		panic("size too big")
	}
	for {
		idx := len(a.blocks) - 1
		block := a.blocks[idx]
		blockOffset := block.alloc(size)
		if blockOffset != nullBlockOffset {
			return newArenaAddr(idx, blockOffset, size)
		}
		return nullArenaAddr
	}
}

func (a *arenaLocator) grow() *arenaLocator {
	newLoc := new(arenaLocator)
	newLoc.blockSize = a.blockSize
	newLoc.blocks = make([]*arenaBlock, 0, len(a.blocks)+1)
	newLoc.blocks = append(newLoc.blocks, a.blocks...)
	newLoc.blocks = append(newLoc.blocks, newArenaBlock(a.blockSize))
	return newLoc
}

type arenaBlock struct {
	buf    []byte
	length uint32
}

func newArenaBlock(blockSize int) *arenaBlock {
	return &arenaBlock{
		buf: make([]byte, blockSize),
	}
}

func (a *arenaBlock) get(offset uint32, size int) []byte {
	if size > 0 {
		return a.buf[offset : offset+uint32(size)]
	}
	return a.buf[offset:]
}

func (a *arenaBlock) alloc(size int) uint32 {
	// The returned addr should be aligned in 8 bytes.
	offset := (int(atomic.LoadUint32(&a.length)) + blockAlign) & alignMask
	length := offset + size
	if length > len(a.buf) {
		return nullBlockOffset
	}
	atomic.StoreUint32(&a.length, uint32(length))
	return uint32(offset)
}
