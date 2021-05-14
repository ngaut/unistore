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
	"math"
	"os"
	"reflect"
	"unsafe"

	"github.com/coocood/bbloom"
	"github.com/dgryski/go-farm"
	"github.com/pingcap/badger/fileutil"
	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/surf"
	"github.com/pingcap/badger/y"
	"golang.org/x/time/rate"
)

type entrySlice struct {
	data    []byte
	endOffs []uint32
}

func (es *entrySlice) append(entry []byte) {
	es.data = append(es.data, entry...)
	es.endOffs = append(es.endOffs, uint32(len(es.data)))
}

func (es *entrySlice) appendVal(val *y.ValueStruct) {
	es.data = val.EncodeTo(es.data)
	es.endOffs = append(es.endOffs, uint32(len(es.data)))
}

func (es *entrySlice) getLast() []byte {
	return es.getEntry(es.length() - 1)
}

func (es *entrySlice) getEntry(i int) []byte {
	var startOff uint32
	if i > 0 {
		startOff = es.endOffs[i-1]
	}
	endOff := es.endOffs[i]
	return es.data[startOff:endOff]
}

func (es *entrySlice) length() int {
	return len(es.endOffs)
}

func (es *entrySlice) size() int {
	return len(es.data) + 4*len(es.endOffs)
}

func (es *entrySlice) reset() {
	es.data = es.data[:0]
	es.endOffs = es.endOffs[:0]
}

const headerSize = 4

// Builder is used in building a table.
type Builder struct {
	counter int // Number of keys written for the current block.

	file          *os.File
	w             tableWriter
	buf           []byte
	writtenLen    int
	rawWrittenLen int

	baseKeys entrySlice

	blockEndOffsets []uint32 // Base offsets of every block.

	// end offsets of every entry within the current block being built.
	// The offsets are relative to the start of the block.
	entryEndOffsets []uint32

	smallest y.Key
	biggest  y.Key

	hashEntries []hashEntry
	bloomFpr    float64
	useGlobalTS bool
	opt         options.TableBuilderOptions
	useSuRF     bool

	surfKeys [][]byte
	surfVals [][]byte

	tmpKeys    entrySlice
	tmpVals    entrySlice
	tmpOldOffs []uint32

	singleKeyOldVers entrySlice
	oldBlock         []byte
}

type tableWriter interface {
	Reset(f *os.File)
	Write(b []byte) (int, error)
	Offset() int64
	Finish() error
}

type inMemWriter struct {
	*bytes.Buffer
}

func (w *inMemWriter) Reset(_ *os.File) {
	w.Buffer.Reset()
}

func (w *inMemWriter) Offset() int64 {
	return int64(w.Len())
}

func (w *inMemWriter) Finish() error {
	return nil
}

// NewTableBuilder makes a new TableBuilder.
// If the f is nil, the builder builds in-memory result.
// If the limiter is nil, the write speed during table build will not be limited.
func NewTableBuilder(f *os.File, limiter *rate.Limiter, level int, opt options.TableBuilderOptions) *Builder {
	t := float64(opt.LevelSizeMultiplier)
	fprBase := math.Pow(t, 1/(t-1)) * opt.LogicalBloomFPR * (t - 1)
	levelFactor := math.Pow(t, float64(opt.MaxLevels-level))
	b := &Builder{
		file:        f,
		buf:         make([]byte, 0, 4*1024),
		hashEntries: make([]hashEntry, 0, 4*1024),
		bloomFpr:    fprBase / levelFactor,
		opt:         opt,
		useSuRF:     level >= opt.SuRFStartLevel,
		// add one byte so the offset would never be 0, so oldOffset is 0 means no old version.
		oldBlock: []byte{0},
	}
	if f != nil {
		b.w = fileutil.NewBufferedWriter(f, opt.WriteBufferSize, limiter)
	} else {
		b.w = &inMemWriter{Buffer: bytes.NewBuffer(make([]byte, 0, opt.MaxTableSize))}
	}
	return b
}

func NewExternalTableBuilder(f *os.File, limiter *rate.Limiter, opt options.TableBuilderOptions) *Builder {
	return &Builder{
		file:        f,
		w:           fileutil.NewBufferedWriter(f, opt.WriteBufferSize, limiter),
		buf:         make([]byte, 0, 4*1024),
		hashEntries: make([]hashEntry, 0, 4*1024),
		bloomFpr:    opt.LogicalBloomFPR,
		useGlobalTS: true,
		opt:         opt,
	}
}

// Reset this builder with new file.
func (b *Builder) Reset(f *os.File) {
	b.file = f
	b.resetBuffers()
	b.w.Reset(f)
}

// SetIsManaged should be called when ingesting a table into a managed DB.
func (b *Builder) SetIsManaged() {
	b.useGlobalTS = false
}

func (b *Builder) resetBuffers() {
	b.counter = 0
	b.buf = b.buf[:0]
	b.writtenLen = 0
	b.rawWrittenLen = 0
	b.baseKeys.reset()
	b.blockEndOffsets = b.blockEndOffsets[:0]
	b.entryEndOffsets = b.entryEndOffsets[:0]
	b.hashEntries = b.hashEntries[:0]
	b.surfKeys = nil
	b.surfVals = nil
	b.smallest.UserKey = b.smallest.UserKey[:0]
	b.biggest.UserKey = b.biggest.UserKey[:0]
	b.oldBlock = b.oldBlock[:0]
}

// Close closes the TableBuilder.
func (b *Builder) Close() {}

// Empty returns whether it's empty.
func (b *Builder) Empty() bool { return b.writtenLen+len(b.buf)+b.tmpKeys.length() == 0 }

// keyDiff returns the first index at which the two keys are different.
func keyDiffIdx(k1, k2 []byte) int {
	var i int
	for i = 0; i < len(k1) && i < len(k2); i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

func (b *Builder) addIndex(key y.Key) {
	if b.smallest.IsEmpty() {
		b.smallest.Copy(key)
	}
	if b.biggest.SameUserKey(key) {
		return
	}
	b.biggest.Copy(key)

	keyHash := farm.Fingerprint64(key.UserKey)
	// It is impossible that a single table contains 16 million keys.
	y.Assert(b.baseKeys.length() < maxBlockCnt)

	pos := entryPosition{uint16(b.baseKeys.length()), uint8(b.counter)}
	if b.useSuRF {
		b.surfKeys = append(b.surfKeys, y.SafeCopy(nil, key.UserKey))
		b.surfVals = append(b.surfVals, pos.encode())
	} else {
		b.hashEntries = append(b.hashEntries, hashEntry{pos, keyHash})
	}
}

func (b *Builder) addHelper(key y.Key, v y.ValueStruct) {
	// Add key to bloom filter.
	if len(key.UserKey) > 0 {
		b.addIndex(key)
	}
	b.tmpKeys.append(key.UserKey)
	v.Version = key.Version
	b.tmpVals.appendVal(&v)
	b.tmpOldOffs = append(b.tmpOldOffs, 0)
	b.counter++
}

// oldEntry format:
//   numEntries(4) | endOffsets(4 * numEntries) | entries
//
// entry format:
//   version(8) | value
func (b *Builder) addOld(key y.Key, v y.ValueStruct) {
	v.Version = key.Version
	keyIdx := b.tmpKeys.length() - 1
	startOff := b.tmpOldOffs[keyIdx]
	if startOff == 0 {
		startOff = uint32(len(b.oldBlock))
		b.tmpOldOffs[keyIdx] = startOff
	}
	b.singleKeyOldVers.appendVal(&v)
}

// entryFormat
// no old entry:
//  diffKeyLen(2) | diffKey | 0 | version(8) | value
// has old entry:
//  diffKeyLen(2) | diffKey | 1 | oldOffset(4) | version(8) | value
func (b *Builder) finishBlock() error {
	if b.tmpKeys.length() == 0 {
		return nil
	}
	if b.singleKeyOldVers.length() > 0 {
		b.flushSingleKeyOldVers()
	}
	firstKey := b.tmpKeys.getEntry(0)
	lastKey := b.tmpKeys.getLast()
	blockCommonLen := keyDiffIdx(firstKey, lastKey)
	for i := 0; i < b.tmpKeys.length(); i++ {
		key := b.tmpKeys.getEntry(i)
		b.buf = appendU16(b.buf, uint16(len(key)-blockCommonLen))
		b.buf = append(b.buf, key[blockCommonLen:]...)
		if b.tmpOldOffs[i] == 0 {
			b.buf = append(b.buf, 0)
		} else {
			b.buf = append(b.buf, 1)
			b.buf = append(b.buf, u32ToBytes(b.tmpOldOffs[i])...)
		}
		b.buf = append(b.buf, b.tmpVals.getEntry(i)...)
		b.entryEndOffsets = append(b.entryEndOffsets, uint32(len(b.buf)))
	}
	b.buf = append(b.buf, U32SliceToBytes(b.entryEndOffsets)...)
	b.buf = append(b.buf, u32ToBytes(uint32(len(b.entryEndOffsets)))...)
	b.buf = appendU16(b.buf, uint16(blockCommonLen))

	// Add base key.
	b.baseKeys.append(firstKey)

	before := b.w.Offset()
	if _, err := b.w.Write(b.buf); err != nil {
		return err
	}
	size := b.w.Offset() - before
	b.blockEndOffsets = append(b.blockEndOffsets, uint32(b.writtenLen+int(size)))
	b.writtenLen += int(size)
	b.rawWrittenLen += len(b.buf)

	// Reset the block for the next build.
	b.entryEndOffsets = b.entryEndOffsets[:0]
	b.counter = 0
	b.buf = b.buf[:0]
	b.tmpKeys.reset()
	b.tmpVals.reset()
	b.tmpOldOffs = b.tmpOldOffs[:0]
	return nil
}

// Add adds a key-value pair to the block.
// If doNotRestart is true, we will not restart even if b.counter >= restartInterval.
func (b *Builder) Add(key y.Key, value y.ValueStruct) error {
	var lastUserKey []byte
	if b.tmpKeys.length() > 0 {
		lastUserKey = b.tmpKeys.getLast()
	}
	// Check old before check finish block, so two blocks never have the same key.
	if bytes.Equal(lastUserKey, key.UserKey) {
		b.addOld(key, value)
		return nil
	} else if b.singleKeyOldVers.length() > 0 {
		b.flushSingleKeyOldVers()
	}
	if b.shouldFinishBlock() {
		if err := b.finishBlock(); err != nil {
			return err
		}
	}
	b.addHelper(key, value)
	return nil // Currently, there is no meaningful error.
}

func (b *Builder) flushSingleKeyOldVers() {
	// numEntries
	b.oldBlock = append(b.oldBlock, u32ToBytes(uint32(b.singleKeyOldVers.length()))...)
	// endOffsets
	b.oldBlock = append(b.oldBlock, U32SliceToBytes(b.singleKeyOldVers.endOffs)...)
	// entries
	b.oldBlock = append(b.oldBlock, b.singleKeyOldVers.data...)
	b.singleKeyOldVers.reset()
}

func (b *Builder) shouldFinishBlock() bool {
	// If there is no entry till now, we will return false.
	if b.tmpKeys.length() == 0 {
		return false
	}
	return uint32(b.tmpKeys.size()+b.tmpVals.size()) > uint32(b.opt.BlockSize)
}

// ReachedCapacity returns true if we... roughly (?) reached capacity?
func (b *Builder) ReachedCapacity(capacity int64) bool {
	estimateSz := b.rawWrittenLen + len(b.buf) +
		4*len(b.blockEndOffsets) + b.baseKeys.size() + len(b.oldBlock)
	return int64(estimateSz) > capacity
}

// EstimateSize returns the size of the SST to build.
func (b *Builder) EstimateSize() int {
	size := b.rawWrittenLen + len(b.buf) + 4*len(b.blockEndOffsets) + b.baseKeys.size() + len(b.oldBlock)
	if !b.useSuRF {
		size += 3 * int(float32(len(b.hashEntries))/b.opt.HashUtilRatio)
	}
	return size
}

const (
	idSmallest byte = iota
	idBiggest
	idBaseKeysEndOffs
	idBaseKeys
	idBlockEndOffsets
	idBloomFilter
	idHashIndex
	idSuRFIndex
	idOldBlockLen
)

// BuildResult contains the build result info, if it's file based compaction, fileName should be used to open Table.
// If it's in memory compaction, FileData and IndexData contains the data.
type BuildResult struct {
	FileName  string
	FileData  []byte
	IndexData []byte
	Smallest  y.Key
	Biggest   y.Key
}

// Finish finishes the table by appending the index.
func (b *Builder) Finish() (*BuildResult, error) {
	err := b.finishBlock() // This will never start a new block.
	if err != nil {
		return nil, err
	}
	if len(b.oldBlock) > 1 {
		_, err = b.w.Write(b.oldBlock)
		if err != nil {
			return nil, err
		}
	}
	if err = b.w.Finish(); err != nil {
		return nil, err
	}
	result := new(BuildResult)
	if b.file != nil {
		idxFile, err := y.OpenTruncFile(IndexFilename(b.file.Name()), false)
		if err != nil {
			return nil, err
		}
		result.FileName = b.file.Name()
		b.w.Reset(idxFile)
	} else {
		result.FileData = y.Copy(b.w.(*inMemWriter).Bytes())
		b.w.Reset(nil)
	}

	// Don't compress the global ts, because it may be updated during ingest.
	ts := uint64(0)
	if b.useGlobalTS {
		// External builder doesn't append ts to the keys, the output sst should has a non-zero global ts.
		ts = 1
	}

	encoder := newMetaEncoder(b.buf, ts)
	encoder.append(b.smallest.UserKey, idSmallest)
	encoder.append(b.biggest.UserKey, idBiggest)
	encoder.append(U32SliceToBytes(b.baseKeys.endOffs), idBaseKeysEndOffs)
	encoder.append(b.baseKeys.data, idBaseKeys)
	encoder.append(U32SliceToBytes(b.blockEndOffsets), idBlockEndOffsets)
	if len(b.oldBlock) > 1 {
		encoder.append(u32ToBytes(uint32(len(b.oldBlock))), idOldBlockLen)
	}

	var bloomFilter []byte
	if !b.useSuRF {
		bf := bbloom.New(float64(len(b.hashEntries)), b.bloomFpr)
		for _, he := range b.hashEntries {
			bf.Add(he.hash)
		}
		bloomFilter = bf.BinaryMarshal()
	}
	encoder.append(bloomFilter, idBloomFilter)

	var hashIndex []byte
	if !b.useSuRF {
		hashIndex = buildHashIndex(b.hashEntries, b.opt.HashUtilRatio)
	}
	encoder.append(hashIndex, idHashIndex)

	var surfIndex []byte
	if b.useSuRF && len(b.surfKeys) > 0 {
		hl := uint32(b.opt.SuRFOptions.HashSuffixLen)
		rl := uint32(b.opt.SuRFOptions.RealSuffixLen)
		sb := surf.NewBuilder(3, hl, rl)
		sf := sb.Build(b.surfKeys, b.surfVals, b.opt.SuRFOptions.BitsPerKeyHint)
		surfIndex = sf.Marshal()
	}
	encoder.append(surfIndex, idSuRFIndex)

	if err = encoder.finish(b.w); err != nil {
		return nil, err
	}

	if err = b.w.Finish(); err != nil {
		return nil, err
	}
	if b.file == nil {
		result.IndexData = y.Copy(b.w.(*inMemWriter).Bytes())
	}
	result.Smallest.Copy(b.smallest)
	result.Biggest.Copy(b.biggest)
	return result, nil
}

func appendU16(buf []byte, v uint16) []byte {
	return append(buf, byte(v), byte(v>>8))
}

func u32ToBytes(v uint32) []byte {
	var uBuf [4]byte
	binary.LittleEndian.PutUint32(uBuf[:], v)
	return uBuf[:]
}

func U64ToBytes(v uint64) []byte {
	var uBuf [8]byte
	binary.LittleEndian.PutUint64(uBuf[:], v)
	return uBuf[:]
}

func U32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(u32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

func BytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

func bytesToU32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func bytesToU64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

type metaEncoder struct {
	buf []byte
}

func newMetaEncoder(buf []byte, globalTS uint64) *metaEncoder {
	buf = append(buf, U64ToBytes(globalTS)...)
	return &metaEncoder{
		buf: buf,
	}
}

func (e *metaEncoder) append(d []byte, id byte) {
	e.buf = append(e.buf, id)
	e.buf = append(e.buf, u32ToBytes(uint32(len(d)))...)
	e.buf = append(e.buf, d...)
}

func (e *metaEncoder) finish(w tableWriter) error {
	_, err := w.Write(e.buf)
	return err
}

type metaDecoder struct {
	buf      []byte
	globalTS uint64

	cursor int
}

func newMetaDecoder(buf []byte) *metaDecoder {
	globalTS := bytesToU64(buf[:8])
	buf = buf[8:]
	return &metaDecoder{
		buf:      buf,
		globalTS: globalTS,
	}
}

func (e *metaDecoder) valid() bool {
	return e.cursor < len(e.buf)
}

func (e *metaDecoder) currentId() byte {
	return e.buf[e.cursor]
}

func (e *metaDecoder) decode() []byte {
	cursor := e.cursor + 1
	l := int(bytesToU32(e.buf[cursor:]))
	cursor += 4
	d := e.buf[cursor : cursor+l]
	return d
}

func (e *metaDecoder) next() {
	l := int(bytesToU32(e.buf[e.cursor+1:]))
	e.cursor += 1 + 4 + l
}
