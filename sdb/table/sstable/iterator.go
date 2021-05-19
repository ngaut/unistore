package sstable

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap/badger/y"
	"io"
	"sort"
)

type blockIterator struct {
	b   *block
	err error
	idx int

	// current entry fields
	diffKey  []byte
	meta     byte
	ver      uint64
	oldVer   uint64
	userMeta []byte
	val      []byte

	// entry index fields
	entryOffs    []uint32
	commonPrefix []byte
	entriesData  []byte
}

func (bi *blockIterator) setBlock(b *block) {
	bi.b = b
	bi.err = nil
	bi.idx = 0
	bi.resetCurrentEntry()
	bi.loadEntries()
}

func (bi *blockIterator) resetCurrentEntry() {
	bi.diffKey = bi.diffKey[:0]
	bi.meta = 0
	bi.ver = 0
	bi.oldVer = 0
	bi.userMeta = bi.userMeta[:0]
	bi.val = bi.val[:0]
}

func (bi *blockIterator) loadEntries() {
	data := bi.b.data
	numEntries := binary.LittleEndian.Uint32(data)
	commonPrefixLenOff := 4 + 4*numEntries
	bi.entryOffs = BytesToU32Slice(data[4:commonPrefixLenOff])
	commonPrefixOff := commonPrefixLenOff + 2
	commonPrefixLen := uint32(binary.LittleEndian.Uint16(data[commonPrefixLenOff:]))
	entriesDataOff := commonPrefixOff + commonPrefixLen
	bi.commonPrefix = data[commonPrefixOff:entriesDataOff]
	bi.entriesData = data[entriesDataOff:]
}

// Seek brings us to the first block element that is >= input key.
// The binary search will begin at `start`, you can use it to skip some items.
func (bi *blockIterator) seek(key []byte) {
	if len(key) <= len(bi.commonPrefix) {
		if bytes.Compare(key, bi.commonPrefix) <= 0 {
			bi.setIdx(0)
		} else {
			bi.setIdx(len(bi.entryOffs))
		}
		return
	}
	cmp := bytes.Compare(key[:len(bi.commonPrefix)], bi.commonPrefix)
	if cmp < 0 {
		bi.setIdx(0)
		return
	} else if cmp > 0 {
		bi.setIdx(len(bi.entryOffs))
		return
	}
	diffKey := key[len(bi.commonPrefix):]
	foundEntryIdx := sort.Search(len(bi.entryOffs), func(idx int) bool {
		bi.setIdx(idx)
		return bytes.Compare(bi.diffKey, diffKey) >= 0
	})
	bi.setIdx(foundEntryIdx)
}

// seekToFirst brings us to the first element. Valid should return true.
func (bi *blockIterator) seekToFirst() {
	bi.setIdx(0)
}

// seekToLast brings us to the last element. Valid should return true.
func (bi *blockIterator) seekToLast() {
	bi.setIdx(len(bi.entryOffs) - 1)
}

func (bi *blockIterator) getEntryData(i int) []byte {
	off := bi.entryOffs[i]
	endOff := uint32(len(bi.entriesData))
	if i+1 < len(bi.entryOffs) {
		endOff = bi.entryOffs[i+1]
	}
	return bi.entriesData[off:endOff]
}

func (bi *blockIterator) setIdx(i int) {
	bi.idx = i
	if i >= len(bi.entryOffs) || i < 0 {
		bi.err = io.EOF
		return
	}
	bi.err = nil
	entryData := bi.getEntryData(i)
	diffKeyLen := binary.LittleEndian.Uint16(entryData)
	entryData = entryData[2:]
	bi.diffKey = entryData[:diffKeyLen]
	entryData = entryData[diffKeyLen:]
	bi.meta = entryData[0]
	entryData = entryData[1:]
	bi.ver = binary.LittleEndian.Uint64(entryData)
	entryData = entryData[8:]
	if bi.meta&metaHasOld != 0 {
		bi.oldVer = binary.LittleEndian.Uint64(entryData)
		entryData = entryData[8:]
	} else {
		bi.oldVer = 0
	}
	userMetaLen := entryData[0]
	entryData = entryData[1:]
	bi.userMeta = entryData[:userMetaLen]
	bi.val = entryData[userMetaLen:]
}

func (bi *blockIterator) next() {
	bi.setIdx(bi.idx + 1)
}

func (bi *blockIterator) prev() {
	bi.setIdx(bi.idx - 1)
}

const (
	iterStateNewVersion = 0
	iterStateOldVer     = 1
	iterStateOldVerDone = 2
)

type Iterator struct {
	t        *Table
	bPos     int
	bi       blockIterator
	oldBPos  int
	obi      blockIterator
	reversed bool
	err      error
	keyBuf   []byte

	iterState int
}

func (itr *Iterator) reset() {
	itr.bPos = 0
	itr.err = nil
	itr.iterState = iterStateNewVersion
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
	itr.reset()
	numBlocks := itr.t.idx.numBlocks()
	if numBlocks == 0 {
		itr.err = io.EOF
		return
	}
	itr.bPos = 0
	block, err := itr.t.loadBlock(itr.bPos)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.setIdx(0)
	itr.syncBlockIterator()
}

func (itr *Iterator) seekToLast() {
	itr.reset()
	numBlocks := itr.t.idx.numBlocks()
	if numBlocks == 0 {
		itr.err = io.EOF
		return
	}
	itr.bPos = numBlocks - 1
	block, err := itr.t.loadBlock(itr.bPos)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.seekToLast()
	itr.syncBlockIterator()
}

func (itr *Iterator) seekInBlock(blockIdx int, key []byte) {
	itr.bPos = blockIdx
	block, err := itr.t.loadBlock(blockIdx)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.seek(key)
	itr.syncBlockIterator()
}

func (itr *Iterator) seekFromOffset(blockIdx int, offset int, key []byte) {
	itr.bPos = blockIdx
	block, err := itr.t.loadBlock(blockIdx)
	if err != nil {
		itr.err = err
		return
	}
	itr.bi.setBlock(block)
	itr.bi.setIdx(offset)
	itr.syncBlockIterator()
	if bytes.Compare(itr.keyBuf, key) >= 0 {
		return
	}
	itr.bi.seek(key)
	itr.syncBlockIterator()
}

// seek brings us to a key that is >= input key.
func (itr *Iterator) seek(key []byte) {
	itr.reset()
	idx := itr.t.idx.seekBlock(key)
	if idx == 0 {
		// The smallest key in our table is already strictly > key. We can return that.
		// This is like a SeekToFirst.
		itr.seekFromOffset(0, 0, key)
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
		if idx == itr.t.idx.numBlocks() {
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

// seekForPrev will reset iterator and seek to <= key.
func (itr *Iterator) seekForPrev(key []byte) {
	// TODO: Optimize this. We shouldn't have to take a Prev step.
	itr.seek(key)
	if !bytes.Equal(itr.Key(), key) {
		itr.prev()
	}
}

func (itr *Iterator) next() {
	itr.err = nil
	itr.iterState = iterStateNewVersion
	if itr.bPos >= itr.t.idx.numBlocks() {
		itr.err = io.EOF
		return
	}
	if itr.bi.b == nil {
		block, err := itr.t.loadBlock(itr.bPos)
		if err != nil {
			itr.err = err
			return
		}
		itr.bi.setBlock(block)
		itr.bi.seekToFirst()
		itr.syncBlockIterator()
		return
	}
	itr.bi.next()
	itr.syncBlockIterator()
	if itr.bi.err != nil {
		itr.bPos++
		itr.bi.b = nil
		itr.next()
	}
}

func (itr *Iterator) prev() {
	itr.err = nil
	itr.iterState = iterStateNewVersion
	if itr.bPos < 0 {
		itr.err = io.EOF
		return
	}

	if itr.bi.b == nil {
		block, err := itr.t.loadBlock(itr.bPos)
		if err != nil {
			itr.err = err
			return
		}
		itr.bi.setBlock(block)
		itr.bi.seekToLast()
		itr.syncBlockIterator()
		return
	}

	itr.bi.prev()
	itr.syncBlockIterator()
	if itr.bi.err != nil {
		itr.bPos--
		itr.bi.b = nil
		itr.prev()
		return
	}
}

// Key follows the y.Iterator interface
func (itr *Iterator) Key() []byte {
	return itr.keyBuf
}

// Value follows the y.Iterator interface
func (itr *Iterator) Value() (ret y.ValueStruct) {
	if itr.iterState == iterStateNewVersion {
		itr.fillValue(&ret, &itr.bi)
	} else {
		itr.fillValue(&ret, &itr.obi)
	}
	return
}

func (itr *Iterator) syncBlockIterator() {
	if itr.bi.err == nil {
		itr.keyBuf = append(itr.keyBuf[:0], itr.bi.commonPrefix...)
		itr.keyBuf = append(itr.keyBuf, itr.bi.diffKey...)
	} else {
		itr.err = itr.bi.err
	}
}

// FillValue fill the value struct.
func (itr *Iterator) FillValue(vs *y.ValueStruct) {
	if itr.iterState == iterStateNewVersion {
		itr.fillValue(vs, &itr.bi)
	} else {
		itr.fillValue(vs, &itr.obi)
	}
}

func (itr *Iterator) fillValue(vs *y.ValueStruct, bi *blockIterator) {
	vs.Meta = bi.meta
	vs.UserMeta = bi.userMeta
	vs.Value = bi.val
	vs.Version = bi.ver
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
	if itr.bi.oldVer == 0 {
		return false
	}
	if itr.iterState == iterStateOldVerDone {
		return false
	}
	if itr.sameOldKey() {
		if itr.iterState == iterStateNewVersion {
			// If it's the first time call, and the key is the same,
			// the old version key must be iterated by a previous key, we should not call next.
			y.Assert(itr.bi.oldVer == itr.obi.ver)
		} else {
			// It's the successive call of NextVersion, we need to move to the next version.
			itr.obi.next()
		}
		if itr.obi.err != nil {
			itr.iterState = iterStateOldVerDone
			return false
		}
		if !itr.sameOldKey() {
			itr.iterState = iterStateOldVerDone
			return false
		}
		itr.iterState = iterStateOldVer
		return true
	}
	itr.err = itr.seekOldBlock()
	y.Assert(itr.err == nil)
	return itr.err == nil
}

func (itr *Iterator) sameOldKey() bool {
	if len(itr.obi.commonPrefix)+len(itr.obi.diffKey) != len(itr.keyBuf) {
		return false
	}
	prefixLen := len(itr.obi.commonPrefix)
	return bytes.Equal(itr.keyBuf[:prefixLen], itr.obi.commonPrefix) &&
		bytes.Equal(itr.keyBuf[prefixLen:], itr.obi.diffKey)
}

func (itr *Iterator) seekOldBlock() error {
	y.Assert(itr.iterState == iterStateNewVersion)
	oldBpos := itr.t.oldIdx.seekBlock(itr.keyBuf) - 1
	if oldBpos == -1 {
		oldBpos = 0
	}
	if itr.obi.b == nil || oldBpos != itr.oldBPos {
		itr.oldBPos = oldBpos
		oldBlock, err := itr.t.loadOldBlock(itr.oldBPos)
		if err != nil {
			return err
		}
		itr.obi.setBlock(oldBlock)
	}
	itr.obi.seek(itr.keyBuf)
	y.Assert(itr.obi.err == nil)
	y.Assert(itr.bi.oldVer == itr.obi.ver)
	itr.iterState = iterStateOldVer
	return nil
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
	return nil
}
