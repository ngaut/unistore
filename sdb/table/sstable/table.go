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
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/coocood/bbloom"
	"github.com/ngaut/unistore/sdb/buffer"
	"github.com/ngaut/unistore/sdb/fileutil"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
)

const (
	fileSuffix    = ".sst"
	idxFileSuffix = ".idx"

	intSize = int(unsafe.Sizeof(int(0)))
)

func IndexFilename(tableFilename string) string { return tableFilename + idxFileSuffix }

type TableIndex struct {
	IndexData       []byte
	globalTS        uint64
	smallest        []byte
	biggest         []byte
	oldOffset       uint32
	blockEndOffsets []uint32
	baseKeys        entrySlice
	bf              *bbloom.Bloom
	hIdx            *hashIndex
}

func NewTableIndex(indexData []byte) *TableIndex {
	idx := newMetaDecoder(indexData).decodeTableIndex()
	idx.IndexData = indexData
	return idx
}

// Table represents a loaded table file with the info we have about it
type Table struct {
	sync.Mutex
	filename string

	globalTs          uint64
	tableSize         int64
	smallest, biggest y.Key
	id                uint64

	file TableFile

	compacting int32

	oldBlockLen int64
}

// Delete delete table's file from disk.
func (t *Table) Delete() error {
	t.file.Delete()
	return nil
}

// OpenTable assumes file has only one table and opens it.  Takes ownership of fd upon function
// entry.  Returns a table with one reference count on it (decrementing which may delete the file!
// -- consider t.Close() instead).  The fd has to writeable because we call Truncate on it before
// deleting.
func OpenTable(filename string, reader TableFile) (*Table, error) {
	id, ok := ParseFileID(filename)
	if !ok {
		return nil, errors.Errorf("Invalid filename: %s", filename)
	}

	t := &Table{
		filename: filename,
		id:       id,
		file:     reader,
	}
	if err := t.initTableInfo(); err != nil {
		t.Close()
		return nil, err
	}
	return t, nil
}

func OpenMMapTable(filename string) (*Table, error) {
	reader, err1 := NewMMapFile(filename)
	if err1 != nil {
		return nil, err1
	}
	return OpenTable(filename, reader)
}

// OpenInMemoryTable opens a table that has data in memory.
func OpenInMemoryTable(reader *InMemFile) (*Table, error) {
	t := &Table{
		file: reader,
	}
	if err := t.initTableInfo(); err != nil {
		return nil, err
	}
	return t, nil
}

// Close closes the open table.  (Releases resources back to the OS.)
func (t *Table) Close() error {
	t.file.Close()
	return nil
}

func (t *Table) NewIterator(reversed bool) y.Iterator {
	return t.newIterator(reversed)
}

func (t *Table) Get(key y.Key, keyHash uint64) (y.ValueStruct, error) {
	resultKey, resultVs, ok, err := t.pointGet(key, keyHash)
	if err != nil {
		return y.ValueStruct{}, err
	}
	if !ok {
		it := t.NewIterator(false)
		defer it.Close()
		it.Seek(key.UserKey)
		if !it.Valid() {
			return y.ValueStruct{}, nil
		}
		if !key.SameUserKey(it.Key()) {
			return y.ValueStruct{}, nil
		}
		resultKey, resultVs = it.Key(), it.Value()
	} else if resultKey.IsEmpty() {
		return y.ValueStruct{}, nil
	}
	result := resultVs
	result.Version = resultKey.Version
	return result, nil
}

// pointGet try to lookup a key and its value by table's hash index.
// If it find an hash collision the last return value will be false,
// which means caller should fallback to seek search. Otherwise it value will be true.
// If the hash index does not contain such an element the returned key will be nil.
func (t *Table) pointGet(key y.Key, keyHash uint64) (y.Key, y.ValueStruct, bool, error) {
	idx, err := t.file.ReadIndex()
	if err != nil {
		return y.Key{}, y.ValueStruct{}, false, err
	}
	if idx.bf != nil && !idx.bf.Has(keyHash) {
		return y.Key{}, y.ValueStruct{}, true, err
	}

	blkIdx, offset := uint32(resultFallback), uint8(0)
	if idx.hIdx != nil {
		blkIdx, offset = idx.hIdx.lookup(keyHash)
	}
	if blkIdx == resultFallback {
		return y.Key{}, y.ValueStruct{}, false, nil
	}
	if blkIdx == resultNoEntry {
		return y.Key{}, y.ValueStruct{}, true, nil
	}

	it := t.newIterator(false)
	defer it.Close()
	it.seekFromOffset(int(blkIdx), int(offset), key.UserKey)

	if !it.Valid() || !key.SameUserKey(it.Key()) {
		return y.Key{}, y.ValueStruct{}, true, it.Error()
	}
	if !y.SeekToVersion(it, key.Version) {
		return y.Key{}, y.ValueStruct{}, true, it.Error()
	}
	return it.Key(), it.Value(), true, nil
}

func (t *Table) initTableInfo() error {
	index, err := t.file.ReadIndex()
	if err != nil {
		return err
	}

	d := newMetaDecoder(index.IndexData)
	t.globalTs = d.globalTS

	for ; d.valid(); d.next() {
		switch d.currentId() {
		case idSmallest:
			if k := d.decode(); len(k) != 0 {
				t.smallest = y.KeyWithTs(y.Copy(k), math.MaxUint64)
			}
		case idBiggest:
			if k := d.decode(); len(k) != 0 {
				t.biggest = y.KeyWithTs(y.Copy(k), 0)
			}
		case idBlockEndOffsets:
			offsets := BytesToU32Slice(d.decode())
			t.tableSize = int64(offsets[len(offsets)-1])
		case idOldBlockLen:
			t.oldBlockLen = int64(bytesToU32(d.decode()))
			t.tableSize += t.oldBlockLen
		}
	}
	return nil
}

func (d *metaDecoder) decodeTableIndex() *TableIndex {
	idx := new(TableIndex)
	for ; d.valid(); d.next() {
		switch d.currentId() {
		case idBaseKeysEndOffs:
			idx.baseKeys.endOffs = BytesToU32Slice(d.decode())
		case idBaseKeys:
			idx.baseKeys.data = d.decode()
		case idBlockEndOffsets:
			idx.blockEndOffsets = BytesToU32Slice(d.decode())
		case idBloomFilter:
			if d := d.decode(); len(d) != 0 {
				idx.bf = new(bbloom.Bloom)
				idx.bf.BinaryUnmarshal(d)
			}
		case idHashIndex:
			if d := d.decode(); len(d) != 0 {
				idx.hIdx = new(hashIndex)
				idx.hIdx.readIndex(d)
			}
		}
	}
	return idx
}

func (t *Table) PrepareForCompaction() error {
	return t.file.LoadToMem()
}

type block struct {
	offset  int
	data    []byte
	baseKey []byte

	reference int32
}

func OnEvict(key uint64, value interface{}) {
	if b, ok := value.(*block); ok {
		b.done()
	}
}

func (b *block) add() (ok bool) {
	for {
		old := atomic.LoadInt32(&b.reference)
		if old == 0 {
			return false
		}
		new := old + 1
		if atomic.CompareAndSwapInt32(&b.reference, old, new) {
			return true
		}
	}
}

func (b *block) done() {
	if b != nil && atomic.AddInt32(&b.reference, -1) == 0 {
		buffer.PutBuffer(b.data)
		b.data = nil
	}
}

func (b *block) size() int64 {
	return int64(intSize + len(b.data))
}

func (t *Table) block(idx int, index *TableIndex) (*block, error) {
	y.Assert(idx >= 0)

	if idx >= len(index.blockEndOffsets) {
		return &block{}, io.EOF
	}
	return t.loadBlock(idx, index)
}

func (t *Table) loadBlock(idx int, index *TableIndex) (*block, error) {
	var startOffset int
	if idx > 0 {
		startOffset = int(index.blockEndOffsets[idx-1])
	}
	endOffset := int(index.blockEndOffsets[idx])
	dataLen := endOffset - startOffset
	var blk *block
	var err error
	if blk, err = t.file.readBlock(int64(startOffset), int64(dataLen)); err != nil {
		return &block{}, errors.Wrapf(err,
			"failed to read from file: %s at offset: %d, len: %d", t.filename, blk.offset, dataLen)
	}
	blk.baseKey = index.baseKeys.getEntry(idx)
	return blk, nil
}

// HasGlobalTs returns table does set global ts.
func (t *Table) HasGlobalTs() bool {
	return t.globalTs != 0
}

// SetGlobalTs update the global ts of external ingested tables.
func (t *Table) SetGlobalTs(ts uint64) error {
	idxFd, err := os.OpenFile(IndexFilename(t.filename), os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if _, err := idxFd.WriteAt(U64ToBytes(ts), 0); err != nil {
		return err
	}
	if err := fileutil.Fsync(idxFd); err != nil {
		return err
	}
	idxFd.Close()
	t.globalTs = ts
	return nil
}

func (t *Table) MarkCompacting(flag bool) {
	if flag {
		atomic.StoreInt32(&t.compacting, 1)
	}
	atomic.StoreInt32(&t.compacting, 0)
}

func (t *Table) IsCompacting() bool {
	return atomic.LoadInt32(&t.compacting) == 1
}

func (t *Table) blockCacheKey(idx int) uint64 {
	y.Assert(t.ID() < math.MaxUint32)
	y.Assert(idx < math.MaxUint32)
	return (t.ID() << 32) | uint64(idx)
}

// Size is its file size in bytes
func (t *Table) Size() int64 { return t.tableSize }

// Smallest is its smallest key, or nil if there are none
func (t *Table) Smallest() y.Key { return t.smallest }

// Biggest is its biggest key, or nil if there are none
func (t *Table) Biggest() y.Key { return t.biggest }

// Filename is NOT the file name.  Just kidding, it is.
func (t *Table) Filename() string { return t.filename }

// ID is the table's ID number (used to make the file name).
func (t *Table) ID() uint64 { return t.id }

func (t *Table) HasOverlap(start, end y.Key, includeEnd bool) bool {
	if start.Compare(t.Biggest()) > 0 {
		return false
	}

	if cmp := end.Compare(t.Smallest()); cmp < 0 {
		return false
	} else if cmp == 0 {
		return includeEnd
	}

	idx, err := t.file.ReadIndex()
	if err != nil {
		return true
	}

	// If there are errors occurred during seeking,
	// we assume the table has overlapped with the range to prevent data loss.
	it := t.newIteratorWithIdx(false, idx)
	defer it.Close()
	it.Seek(start.UserKey)
	if !it.Valid() {
		return it.Error() != nil
	}
	if cmp := it.Key().Compare(end); cmp > 0 {
		return false
	} else if cmp == 0 {
		return includeEnd
	}
	return true
}

// ParseFileID reads the file id out of a filename.
func ParseFileID(name string) (uint64, bool) {
	name = path.Base(name)
	if !strings.HasSuffix(name, fileSuffix) {
		return 0, false
	}
	//	suffix := name[len(fileSuffix):]
	name = strings.TrimSuffix(name, fileSuffix)
	id, err := strconv.ParseUint(name, 16, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// IDToFilename does the inverse of ParseFileID
func IDToFilename(id uint64) string {
	return fmt.Sprintf("%08x", id) + fileSuffix
}

// NewFilename should be named TableFilepath -- it combines the dir with the ID to make a table
// filepath.
func NewFilename(id uint64, dir string) string {
	return filepath.Join(dir, IDToFilename(id))
}
