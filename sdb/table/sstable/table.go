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
	"fmt"
	"github.com/coocood/bbloom"
	"github.com/ngaut/unistore/sdb/buffer"
	"github.com/ngaut/unistore/sdb/cache"
	"github.com/ngaut/unistore/sdb/table"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"hash/crc32"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"
)

const (
	fileSuffix = ".sst"
	intSize    = int(unsafe.Sizeof(int(0)))
)

// OpenTable assumes file has only one table and opens it.  Takes ownership of fd upon function
// entry.  Returns a table with one reference count on it (decrementing which may delete the file!
// -- consider t.Close() instead).  The fd has to writeable because we call Truncate on it before
// deleting.
func OpenTable(file TableFile, cache *cache.Cache) (*Table, error) {
	t := &Table{
		TableFile: file,
		cache:     cache,
	}
	if file.Size() < int64(footerSize) {
		return nil, errors.Errorf("invalid file size %d", file.Size())
	}
	footerData := make([]byte, footerSize)
	_, err := file.ReadAt(footerData, file.Size()-int64(footerSize))
	if err != nil {
		return nil, err
	}
	t.footer.unmarshal(footerData)
	if t.footer.magic != MagicNumber {
		return nil, errors.Errorf("magic number not match expect %d got %d", MagicNumber, t.footer.magic)
	}
	var indexData, oldIndexData, propsData []byte
	if file.IsMMap() {
		indexData = file.MMapRead(int64(t.footer.indexOffset), t.footer.indexLen())
		oldIndexData = file.MMapRead(int64(t.footer.oldIndexOffset), t.footer.oldIndexLen())
		propsData = file.MMapRead(int64(t.footer.propertiesOffset), t.footer.propertiesLen(file.Size()))
	} else {
		indexData = make([]byte, t.footer.indexLen())
		_, err = file.ReadAt(indexData, int64(t.footer.indexOffset))
		if err != nil {
			return nil, err
		}
		oldIndexData = make([]byte, t.footer.oldIndexLen())
		_, err = file.ReadAt(oldIndexData, int64(t.footer.oldIndexOffset))
		if err != nil {
			return nil, err
		}
		propsData = make([]byte, t.footer.propertiesLen(file.Size()))
		_, err = file.ReadAt(propsData, int64(t.footer.propertiesOffset))
		if err != nil {
			return nil, err
		}
	}
	t.idx, err = newIndex(indexData, t.footer.checksumType)
	if err != nil {
		return nil, err
	}
	t.oldIdx, err = newIndex(oldIndexData, t.footer.checksumType)
	if err != nil {
		return nil, err
	}
	if err = validateChecksum(propsData, t.footer.checksumType); err != nil {
		return nil, err
	}
	propsData = propsData[4:]
	var key string
	var val []byte
	for len(propsData) > 0 {
		key, val, propsData = parsePropData(propsData)
		switch key {
		case propKeySmallest:
			t.smallest = val
		case propKeyBiggest:
			t.biggest = val
		}
	}
	return t, nil
}

func parsePropData(propsData []byte) (key string, val, remained []byte) {
	keyLen := binary.LittleEndian.Uint16(propsData)
	propsData = propsData[2:]
	key = string(propsData[:keyLen])
	propsData = propsData[keyLen:]
	valLen := binary.LittleEndian.Uint32(propsData)
	propsData = propsData[4:]
	val = propsData[:valLen]
	propsData = propsData[valLen:]
	remained = propsData
	return
}

type block struct {
	data      []byte
	reference int32
}

func OnEvict(key cache.Key, value interface{}) {
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

type Table struct {
	TableFile
	cache    *cache.Cache
	footer   footer
	smallest []byte
	biggest  []byte
	idx      *nIndex
	oldIdx   *nIndex
}

func (t *Table) PrepareForCompaction() error {
	return t.LoadToRam()
}

func (t *Table) loadBlock(pos int) (*block, error) {
	addr := t.idx.blockAddrs[pos]
	var length int
	if pos+1 < t.idx.numBlocks() {
		length = int(t.idx.blockAddrs[pos+1].currentOff - addr.currentOff)
	} else {
		length = t.footer.dataLen() - int(addr.currentOff)
	}
	return t.loadBlockByAddrLen(addr, length)
}

func (t *Table) loadOldBlock(pos int) (*block, error) {
	addr := t.oldIdx.blockAddrs[pos]
	var length int
	if pos+1 < t.oldIdx.numBlocks() {
		length = int(t.oldIdx.blockAddrs[pos+1].currentOff - addr.currentOff)
	} else {
		length = int(t.footer.indexOffset - addr.currentOff)
	}
	return t.loadBlockByAddrLen(addr, length)
}

func (t *Table) loadBlockByAddrLen(addr blockAddress, length int) (*block, error) {
	if t.IsMMap() {
		data := t.MMapRead(int64(addr.currentOff), length)
		return &block{data: data[4:]}, nil
	}
	return t.readBlock(addr, length)
}

func (t *Table) readBlock(addr blockAddress, length int) (*block, error) {
	for {
		blk, err := t.cache.GetOrCompute(blockCacheKey(addr.originFID, addr.originOff), func() (interface{}, int64, error) {
			data := buffer.GetBuffer(length)
			if _, err := t.ReadAt(data, int64(addr.currentOff)); err != nil {
				buffer.PutBuffer(data)
				return nil, 0, err
			}
			if err := validateChecksum(data, t.footer.checksumType); err != nil {
				return nil, 0, err
			}
			blk := &block{
				data:      data[4:],
				reference: 1,
			}
			return blk, int64(length), nil
		})
		if err != nil {
			return nil, err
		}
		b := blk.(*block)
		// When a block is evicted, the block counter fail to add reference.
		if ok := b.add(); !ok {
			// Add reference failed, so try to read block again until success.
			continue
		}
		return b, nil
	}
}

func (t *Table) NewIterator(reversed bool) table.Iterator {
	return &Iterator{t: t, reversed: reversed}
}

func (t *Table) Get(key []byte, version, keyHash uint64) (y.ValueStruct, error) {
	if t.idx.bloom != nil {
		if !t.idx.bloom.Has(keyHash) {
			return y.ValueStruct{}, nil
		}
	}
	it := t.NewIterator(false)
	defer it.Close()
	it.Seek(key)
	if !it.Valid() || !bytes.Equal(key, it.Key()) {
		return y.ValueStruct{}, nil
	}
	for it.Value().Version > version {
		if !it.NextVersion() {
			return y.ValueStruct{}, nil
		}
	}
	return cloneValue(it.Value()), nil
}

func cloneValue(v y.ValueStruct) y.ValueStruct {
	bin := v.EncodeTo(make([]byte, 0, v.EncodedSize()))
	var n y.ValueStruct
	n.Decode(bin)
	return n
}

func (t *Table) Smallest() []byte {
	return t.smallest
}

func (t *Table) Biggest() []byte {
	return t.biggest
}

func (t *Table) HasOverlap(start, end []byte, includeEnd bool) bool {
	if bytes.Compare(start, t.Biggest()) > 0 {
		return false
	}

	if cmp := bytes.Compare(end, t.Smallest()); cmp < 0 {
		return false
	} else if cmp == 0 {
		return includeEnd
	}
	it := t.NewIterator(false).(*Iterator)
	defer it.Close()
	it.Seek(start)
	if !it.Valid() {
		return it.err != nil
	}
	if cmp := bytes.Compare(it.Key(), end); cmp > 0 {
		return false
	} else if cmp == 0 {
		return includeEnd
	}
	return true
}

type nIndex struct {
	commonPrefix []byte
	blockKeyOffs []uint32
	blockAddrs   []blockAddress
	blockKeys    []byte
	data         []byte
	bloom        *bbloom.Bloom
}

func (idx *nIndex) numBlocks() int {
	return len(idx.blockKeyOffs)
}

func (idx *nIndex) seekBlock(key []byte) int {
	if len(key) <= len(idx.commonPrefix) {
		if bytes.Compare(key, idx.commonPrefix) <= 0 {
			return 0
		}
		return len(idx.blockKeyOffs)
	}
	cmp := bytes.Compare(key[:len(idx.commonPrefix)], idx.commonPrefix)
	if cmp < 0 {
		return 0
	} else if cmp > 0 {
		return len(idx.blockKeyOffs)
	}
	diffKey := key[len(idx.commonPrefix):]
	return sort.Search(len(idx.blockKeyOffs), func(i int) bool {
		return bytes.Compare(idx.blockDiffKey(i), diffKey) > 0
	})
}

func (idx *nIndex) blockDiffKey(i int) []byte {
	off := idx.blockKeyOffs[i]
	endOff := uint32(len(idx.blockKeys))
	if i+1 < len(idx.blockKeyOffs) {
		endOff = idx.blockKeyOffs[i+1]
	}
	return idx.blockKeys[off:endOff]
}

func newIndex(data []byte, checksumType byte) (*nIndex, error) {
	idx := &nIndex{data: data}
	if err := validateChecksum(data, checksumType); err != nil {
		return nil, err
	}
	off := 4
	numBlocks := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	idx.blockKeyOffs = BytesToU32Slice(data[off : off+4*numBlocks])
	off += 4 * numBlocks
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&idx.blockAddrs))
	hdr.Len = numBlocks
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&data[off]))
	off += blockAddrSize * numBlocks
	commonPrefixLen := int(binary.LittleEndian.Uint16(data[off:]))
	off += 2
	idx.commonPrefix = data[off : off+commonPrefixLen]
	off += commonPrefixLen
	blockKeysLen := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	idx.blockKeys = data[off : off+blockKeysLen]
	off += blockKeysLen
	for len(data[off:]) > 0 {
		auxType := binary.LittleEndian.Uint16(data[off:])
		off += 2
		auxLen := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		auxData := data[off : off+auxLen]
		off += auxLen
		switch auxType {
		case bloomFilter:
			idx.bloom = new(bbloom.Bloom)
			idx.bloom.BinaryUnmarshal(auxData)
		}
	}
	return idx, nil
}

func validateChecksum(data []byte, checksumType byte) error {
	if len(data) < 4 {
		return errors.New("data is too short")
	}
	checksum := binary.LittleEndian.Uint32(data)
	content := data[4:]
	if checksumType == crc32Castagnoli {
		gotChecksum := crc32.Checksum(content, crc32.MakeTable(crc32.Castagnoli))
		if checksum != gotChecksum {
			return errors.Errorf("checksum mismatch expect %d got %d", checksum, gotChecksum)
		}
	}
	return nil
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
