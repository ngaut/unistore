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

package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"sort"

	"github.com/pingcap/badger/y"
)

type singleKeyIterator struct {
	oldOffset uint32
	loaded    bool
	latestVal []byte
	oldVals   entrySlice
	idx       int

	oldBlock *block
}

func (ski *singleKeyIterator) set(oldOffset uint32, latestVal []byte) {
	ski.oldOffset = oldOffset
	ski.latestVal = latestVal
	ski.loaded = false
	ski.idx = 0
}

func (ski *singleKeyIterator) getVal() (val []byte) {
	if ski.idx == 0 {
		return ski.latestVal
	}
	oldEntry := ski.oldVals.getEntry(ski.idx - 1)
	return oldEntry
}

func (ski *singleKeyIterator) loadOld(oldBlock *block) {
	data := oldBlock.data
	numEntries := bytesToU32(data[ski.oldOffset:])
	endOffsStartIdx := ski.oldOffset + 4
	endOffsEndIdx := endOffsStartIdx + 4*numEntries
	ski.oldVals.endOffs = BytesToU32Slice(data[endOffsStartIdx:endOffsEndIdx])
	valueEndOff := endOffsEndIdx + ski.oldVals.endOffs[numEntries-1]
	ski.oldVals.data = data[endOffsEndIdx:valueEndOff]
	ski.loaded = true
}

func (ski *singleKeyIterator) length() int {
	return ski.oldVals.length() + 1
}

func (ski *singleKeyIterator) close() {
	if ski.oldBlock != nil {
		ski.oldBlock.done()
		ski.oldBlock = nil
	}
}

type blockIterator struct {
	entries entrySlice
	idx     int
	err     error

	globalTsBytes [8]byte
	globalTs      uint64
	key           y.Key
	val           []byte

	baseLen uint16
	ski     singleKeyIterator

	block *block
}

func (itr *blockIterator) setBlock(b *block) {
	itr.block.done()
	itr.block = b
	itr.err = nil
	itr.idx = 0
	itr.key.Reset()
	itr.val = itr.val[:0]
	itr.loadEntries(b.data)
	itr.key.UserKey = append(itr.key.UserKey[:0], b.baseKey[:itr.baseLen]...)
}

func (itr *blockIterator) valid() bool {
	return itr != nil && itr.err == nil
}

func (itr *blockIterator) Error() error {
	return itr.err
}

// loadEntries loads the entryEndOffsets for binary searching for a key.
func (itr *blockIterator) loadEntries(data []byte) {
	// Get the number of entries from the end of `data` (and remove it).
	dataLen := len(data)
	itr.baseLen = binary.LittleEndian.Uint16(data[dataLen-2:])
	entriesNum := int(bytesToU32(data[dataLen-6:]))
	entriesEnd := dataLen - 6
	entriesStart := entriesEnd - entriesNum*4
	itr.entries.endOffs = BytesToU32Slice(data[entriesStart:entriesEnd])
	itr.entries.data = data[:entriesStart]
}

// Seek brings us to the first block element that is >= input key.
// The binary search will begin at `start`, you can use it to skip some items.
func (itr *blockIterator) seek(key []byte) {
	foundEntryIdx := sort.Search(itr.entries.length(), func(idx int) bool {
		itr.setIdx(idx)
		return bytes.Compare(itr.key.UserKey, key) >= 0
	})
	itr.setIdx(foundEntryIdx)
}

// seekToFirst brings us to the first element. Valid should return true.
func (itr *blockIterator) seekToFirst() {
	itr.setIdx(0)
}

// seekToLast brings us to the last element. Valid should return true.
func (itr *blockIterator) seekToLast() {
	itr.setIdx(itr.entries.length() - 1)
}

// setIdx sets the iterator to the entry index and set the current key and value.
func (itr *blockIterator) setIdx(i int) {
	itr.idx = i
	if i >= itr.entries.length() || i < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil
	entryData := itr.entries.getEntry(i)
	diffKeyLen := binary.LittleEndian.Uint16(entryData)
	entryData = entryData[2:]
	itr.key.UserKey = append(itr.key.UserKey[:itr.baseLen], entryData[:diffKeyLen]...)
	entryData = entryData[diffKeyLen:]
	hasOld := entryData[0] != 0
	entryData = entryData[1:]
	var oldOffset uint32
	if hasOld {
		oldOffset = bytesToU32(entryData)
		entryData = entryData[4:]
	}
	if itr.globalTs != 0 {
		itr.key.Version = itr.globalTs
	} else {
		itr.key.Version = bytesToU64(entryData)
	}
	itr.val = entryData
	itr.ski.set(oldOffset, itr.val)
}

func (itr *blockIterator) hasOldVersion() bool {
	return itr.ski.oldOffset != 0
}

func (itr *blockIterator) next() {
	itr.setIdx(itr.idx + 1)
}

func (itr *blockIterator) prev() {
	itr.setIdx(itr.idx - 1)
}

func (itr *blockIterator) close() {
	itr.block.done()
	itr.ski.close()
}

// Iterator is an iterator for a Table.
type Iterator struct {
	t    *Table
	tIdx *TableIndex
	bpos int
	bi   blockIterator
	err  error

	// Internally, Iterator is bidirectional. However, we only expose the
	// unidirectional functionality for now.
	reversed bool
}

// NewIterator returns a new iterator of the Table
func (t *Table) newIterator(reversed bool) *Iterator {
	idx, err := t.file.ReadIndex()
	if err != nil {
		return &Iterator{err: err}
	}
	return t.newIteratorWithIdx(reversed, idx)
}

func (t *Table) newIteratorWithIdx(reversed bool, index *TableIndex) *Iterator {
	it := &Iterator{t: t, reversed: reversed, tIdx: index}
	it.bi.globalTs = t.globalTs
	binary.BigEndian.PutUint64(it.bi.globalTsBytes[:], math.MaxUint64-t.globalTs)
	return it
}

func (itr *Iterator) reset() {
	itr.bpos = 0
	itr.err = nil
}

// Valid follows the y.Iterator interface
func (itr *Iterator) Valid() bool {
	return itr.err == nil
}

func (itr *Iterator) Error() error {
	if itr.err == io.EOF {
		return nil
	}
	return itr.err
}

func (itr *Iterator) seekToFirst() {
	numBlocks := len(itr.tIdx.blockEndOffsets)
	if numBlocks == 0 {
		itr.err = io.EOF
		return
	}
	itr.bpos = 0
	block, err := itr.t.block(itr.bpos, itr.tIdx)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.seekToFirst()
	itr.err = itr.bi.Error()
}

func (itr *Iterator) seekToLast() {
	numBlocks := len(itr.tIdx.blockEndOffsets)
	if numBlocks == 0 {
		itr.err = io.EOF
		return
	}
	itr.bpos = numBlocks - 1
	block, err := itr.t.block(itr.bpos, itr.tIdx)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.seekToLast()
	itr.err = itr.bi.Error()
}

func (itr *Iterator) seekInBlock(blockIdx int, key []byte) {
	itr.bpos = blockIdx
	block, err := itr.t.block(blockIdx, itr.tIdx)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.seek(key)
	itr.err = itr.bi.Error()
}

func (itr *Iterator) seekFromOffset(blockIdx int, offset int, key []byte) {
	itr.bpos = blockIdx
	block, err := itr.t.block(blockIdx, itr.tIdx)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.setIdx(offset)
	if bytes.Compare(itr.bi.key.UserKey, key) >= 0 {
		return
	}
	itr.bi.seek(key)
	itr.err = itr.bi.err
}

func (itr *Iterator) seekBlock(key []byte) int {
	return sort.Search(len(itr.tIdx.blockEndOffsets), func(idx int) bool {
		blockBaseKey := itr.tIdx.baseKeys.getEntry(idx)
		return bytes.Compare(blockBaseKey, key) > 0
	})
}

// seekFrom brings us to a key that is >= input key.
func (itr *Iterator) seekFrom(key []byte) {
	itr.err = nil
	itr.reset()

	idx := itr.seekBlock(key)
	if itr.err != nil {
		return
	}
	if idx == 0 {
		// The smallest key in our table is already strictly > key. We can return that.
		// This is like a SeekToFirst.
		itr.seekInBlock(0, key)
		return
	}

	// block[idx].smallest is > key.
	// Since idx>0, we know block[idx-1].smallest is <= key.
	// There are two cases.
	// 1) Everything in block[idx-1] is strictly < key. In this case, we should go to the first
	//    element of block[idx].
	// 2) Some element in block[idx-1] is >= key. We should go to that element.
	itr.seekInBlock(idx-1, key)
	if itr.err == io.EOF {
		// Case 1. Need to visit block[idx].
		if idx == len(itr.tIdx.blockEndOffsets) {
			// If idx == len(itr.t.blockEndOffsets), then input key is greater than ANY element of table.
			// There's nothing we can do. Valid() should return false as we seek to end of table.
			return
		}
		itr.err = nil
		// Since block[idx].smallest is > key. This is essentially a block[idx].SeekToFirst.
		itr.seekFromOffset(idx, 0, key)
	}
	// Case 2: No need to do anything. We already did the seek in block[idx-1].
}

// seek will reset iterator and seek to >= key.
func (itr *Iterator) seek(key []byte) {
	itr.err = nil
	itr.reset()
	itr.seekFrom(key)
}

// seekForPrev will reset iterator and seek to <= key.
func (itr *Iterator) seekForPrev(key []byte) {
	// TODO: Optimize this. We shouldn't have to take a Prev step.
	itr.seekFrom(key)
	if !bytes.Equal(itr.Key().UserKey, key) {
		itr.prev()
	}
}

func (itr *Iterator) next() {
	itr.err = nil

	if itr.bpos >= len(itr.tIdx.blockEndOffsets) {
		itr.err = io.EOF
		return
	}

	if itr.bi.entries.length() == 0 {
		block, err := itr.t.block(itr.bpos, itr.tIdx)
		if err != nil {
			itr.err = err
			return
		}
		itr.bi.setBlock(block)
		itr.bi.seekToFirst()
		itr.err = itr.bi.Error()
		return
	}

	itr.bi.next()
	if !itr.bi.valid() {
		itr.bpos++
		itr.bi.entries.reset()
		itr.next()
		return
	}
}

func (itr *Iterator) prev() {
	itr.err = nil
	if itr.bpos < 0 {
		itr.err = io.EOF
		return
	}

	if itr.bi.entries.length() == 0 {
		block, err := itr.t.block(itr.bpos, itr.tIdx)
		if err != nil {
			itr.err = err
			return
		}
		itr.bi.setBlock(block)
		itr.bi.seekToLast()
		itr.err = itr.bi.Error()
		return
	}

	itr.bi.prev()
	if !itr.bi.valid() {
		itr.bpos--
		itr.bi.entries.reset()
		itr.prev()
		return
	}
}

// Key follows the y.Iterator interface
func (itr *Iterator) Key() y.Key {
	return itr.bi.key
}

// Value follows the y.Iterator interface
func (itr *Iterator) Value() (ret y.ValueStruct) {
	ret.Decode(itr.bi.val)
	return
}

// FillValue fill the value struct.
func (itr *Iterator) FillValue(vs *y.ValueStruct) {
	vs.Decode(itr.bi.val)
}

// Next follows the y.Iterator interface
func (itr *Iterator) Next() {
	if !itr.reversed {
		itr.next()
	} else {
		itr.prev()
	}
}

func (itr *Iterator) NextVersion() bool {
	if itr.bi.ski.oldOffset == 0 {
		return false
	}
	if !itr.bi.ski.loaded {
		oldOffset := itr.t.tableSize - itr.t.oldBlockLen
		oldBlock, err := itr.t.file.readBlock(oldOffset, itr.t.oldBlockLen)
		if err != nil {
			itr.err = err
			return false
		}
		itr.bi.ski.loadOld(oldBlock)
	}
	if itr.bi.ski.idx+1 < itr.bi.ski.length() {
		itr.bi.ski.idx++
		itr.bi.val = itr.bi.ski.getVal()
		itr.bi.key.Version = bytesToU64(itr.bi.val)
		return true
	}
	return false
}

// Rewind follows the y.Iterator interface
func (itr *Iterator) Rewind() {
	if !itr.reversed {
		itr.seekToFirst()
	} else {
		itr.seekToLast()
	}
}

// Seek follows the y.Iterator interface
func (itr *Iterator) Seek(key []byte) {
	if !itr.reversed {
		itr.seek(key)
	} else {
		itr.seekForPrev(key)
	}
}

// Close closes the iterator (and it must be called).
func (itr *Iterator) Close() error {
	itr.bi.close()
	return nil
}
