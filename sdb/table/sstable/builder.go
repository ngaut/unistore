package sstable

import (
	"bytes"
	"encoding/binary"
	"github.com/coocood/bbloom"
	"github.com/dgryski/go-farm"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"hash/crc32"
	"io"
	"reflect"
	"unsafe"
)

const (
	footerSize    = int(unsafe.Sizeof(footer{}))
	blockAddrSize = int(unsafe.Sizeof(blockAddress{}))
	metaHasOld    = byte(1 << 1)
	bloomFilter   = uint16(1)

	noChecksum      = 0
	crc32Castagnoli = 1
	tableFormat     = 1
	MagicNumber     = 2940551257
	noCompression   = 0
	propKeySmallest = "smallest"
	propKeyBiggest  = "biggest"
)

type TableBuilderOptions struct {
	HashUtilRatio       float32
	WriteBufferSize     int
	BytesPerSecond      int
	MaxLevels           int
	LevelSizeMultiplier int
	LogicalBloomFPR     float64
	BlockSize           int
	MaxTableSize        int64
}

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

// Builder is used in building a table.
type Builder struct {
	fid        uint64
	propKeys   []string
	propValues [][]byte

	blockBuilder blockBuilder
	oldBuilder   blockBuilder
	blockSize    int
	bloomFpr     float64
	checksumType byte
	keyHashes    []uint64
	smallest     []byte
	biggest      []byte
}

// NewTableBuilder makes a new TableBuilder.
// If the f is nil, the builder builds in-memory result.
// If the limiter is nil, the write speed during table build will not be limited.
func NewTableBuilder(fid uint64, opt TableBuilderOptions) *Builder {
	return &Builder{
		fid:          fid,
		blockSize:    opt.BlockSize,
		bloomFpr:     opt.LogicalBloomFPR,
		checksumType: crc32Castagnoli,
	}
}

func (b *Builder) Reset(fid uint64) {
	b.fid = fid
	b.blockBuilder.resetAll()
	b.oldBuilder.resetAll()
	b.propKeys = b.propKeys[:0]
	b.propValues = b.propValues[:0]
	b.keyHashes = b.keyHashes[:0]
	b.smallest = b.smallest[:0]
	b.biggest = b.biggest[:0]
}

func (b *Builder) addProperty(key string, val []byte) {
	b.propKeys = append(b.propKeys, key)
	b.propValues = append(b.propValues, val)
}

func (b *Builder) Add(key []byte, val *y.ValueStruct) {
	if b.blockBuilder.sameLastKey(key) {
		b.blockBuilder.setLastEntryOldVersionIfZero(val.Version)
		b.oldBuilder.addEntry(key, val)
	} else {
		// Only try to finish block when the key is different than last.
		if b.blockBuilder.block.size > b.blockSize {
			b.blockBuilder.finishBlock(b.fid, b.checksumType)
		}
		if b.oldBuilder.block.size > b.blockSize {
			b.oldBuilder.finishBlock(b.fid, b.checksumType)
		}
		b.blockBuilder.addEntry(key, val)
		b.keyHashes = append(b.keyHashes, farm.Fingerprint64(key))
		if len(b.smallest) == 0 {
			b.smallest = y.Copy(key)
		}
	}
}

func (b *Builder) EstimateSize() int {
	return len(b.blockBuilder.buf) + len(b.oldBuilder.buf) + b.blockBuilder.block.size + b.oldBuilder.block.size
}

type bytesWriter interface {
	Bytes() []byte
}

func (b *Builder) Finish(filename string, w io.Writer) (*BuildResult, error) {
	if b.blockBuilder.block.size > 0 {
		b.biggest = y.Copy(b.blockBuilder.block.tmpKeys.getLast())
		b.blockBuilder.finishBlock(b.fid, b.checksumType)
	}
	if b.oldBuilder.block.size > 0 {
		b.oldBuilder.finishBlock(b.fid, b.checksumType)
	}
	y.Assert(b.blockBuilder.blockKeys.length() > 0)
	dataSectionSize, err := w.Write(b.blockBuilder.buf)
	if err != nil {
		return nil, err
	}
	oldDataSectionSize, err := w.Write(b.oldBuilder.buf)
	if err != nil {
		return nil, err
	}
	b.blockBuilder.buildIndex(0, b.keyHashes, b.bloomFpr, b.checksumType)
	indexSectionSize, err := w.Write(b.blockBuilder.buf)
	if err != nil {
		return nil, err
	}
	b.oldBuilder.buildIndex(uint32(dataSectionSize), nil, 0, b.checksumType)
	oldIndexSectionSize, err := w.Write(b.oldBuilder.buf)
	if err != nil {
		return nil, err
	}
	buf := b.blockBuilder.buf[:0]
	buf = b.buildProperties(buf)
	_, err = w.Write(buf)
	if err != nil {
		return nil, err
	}
	var footer footer
	footer.oldDataOffset = uint32(dataSectionSize)
	footer.indexOffset = footer.oldDataOffset + uint32(oldDataSectionSize)
	footer.oldIndexOffset = footer.indexOffset + uint32(indexSectionSize)
	footer.propertiesOffset = footer.oldIndexOffset + uint32(oldIndexSectionSize)
	footer.compressionType = noCompression
	footer.checksumType = b.checksumType
	footer.tableFormatVersion = tableFormat
	footer.magic = MagicNumber
	_, err = w.Write(footer.marshal())
	if err != nil {
		return nil, err
	}
	result := &BuildResult{
		FileName: filename,
		Smallest: b.smallest,
		Biggest:  b.biggest,
	}
	if x, ok := w.(bytesWriter); ok {
		result.FileData = x.Bytes()
	}
	return result, nil
}

func (b *Builder) buildProperties(buf []byte) []byte {
	buf = AppendU32(buf, 0)
	b.addProperty(propKeySmallest, b.smallest)
	b.addProperty(propKeyBiggest, b.biggest)
	for i, key := range b.propKeys {
		buf = AppendU16(buf, uint16(len(key)))
		buf = append(buf, key...)
		val := b.propValues[i]
		buf = AppendU32(buf, uint32(len(val)))
		buf = append(buf, val...)
	}
	if b.checksumType == crc32Castagnoli {
		binary.LittleEndian.PutUint32(buf, crc32.Checksum(buf[4:], crc32.MakeTable(crc32.Castagnoli)))
	}
	return buf
}

// Empty returns whether it's empty.
func (b *Builder) Empty() bool { return len(b.smallest) == 0 }

type footer struct {
	oldDataOffset      uint32
	indexOffset        uint32
	oldIndexOffset     uint32
	propertiesOffset   uint32
	compressionType    byte
	checksumType       byte
	tableFormatVersion uint16
	magic              uint32
}

func (f *footer) marshal() []byte {
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = footerSize
	hdr.Cap = footerSize
	hdr.Data = uintptr(unsafe.Pointer(f))
	return b
}

func (f *footer) unmarshal(b []byte) {
	y.Assert(len(b) == footerSize)
	*f = *(*footer)(unsafe.Pointer(&b[0]))
}

func (f *footer) dataLen() int {
	return int(f.oldDataOffset)
}

func (f *footer) oldDataLen() int {
	return int(f.indexOffset - f.oldDataOffset)
}

func (f *footer) indexLen() int {
	return int(f.oldIndexOffset - f.indexOffset)
}

func (f *footer) oldIndexLen() int {
	return int(f.propertiesOffset - f.oldIndexOffset)
}

func (f *footer) propertiesLen(tableSize int64) int {
	return int(tableSize) - int(f.propertiesOffset) - footerSize
}

func (f *footer) validate(tableSize int64) error {
	// make sure each section has sufficient length.
	if f.dataLen()+f.oldDataLen()+f.indexLen()+f.oldIndexLen()+f.propertiesLen(tableSize)+
		footerSize != int(tableSize) {
		return errors.Errorf("invalid footer %v", f)
	}
	return nil
}

type blockBuffer struct {
	tmpKeys    entrySlice
	tmpVals    entrySlice
	oldVers    []uint64
	entrySizes []uint32
	size       int
}

type blockAddress struct {
	originFID  uint64
	originOff  uint32
	currentOff uint32
}

type blockBuilder struct {
	buf        []byte
	block      blockBuffer
	blockKeys  entrySlice
	blockAddrs []blockAddress
}

func (b *blockBuilder) sameLastKey(key []byte) bool {
	if b.block.tmpKeys.length() > 0 {
		last := b.block.tmpKeys.getLast()
		return bytes.Equal(last, key)
	}
	return false
}

func (b *blockBuilder) setLastEntryOldVersionIfZero(oldVersion uint64) {
	if b.block.oldVers[len(b.block.oldVers)-1] == 0 {
		b.block.oldVers[len(b.block.oldVers)-1] = oldVersion
		b.block.entrySizes[len(b.block.entrySizes)-1] += 8
		b.block.size += 8
	}
}

func (b *blockBuilder) addEntry(key []byte, val *y.ValueStruct) {
	b.block.tmpKeys.append(key)
	b.block.tmpVals.appendVal(val)
	b.block.oldVers = append(b.block.oldVers, 0)
	// key_suffix_len(2) + len(key) + meta(1) + version(8) + user_meta_len(1) + len(user_meta) + len(value)
	entrySize := 2 + len(key) + 1 + 8 + 1 + len(val.UserMeta) + len(val.Value)
	b.block.entrySizes = append(b.block.entrySizes, uint32(entrySize))
	b.block.size += entrySize
}

func (b *blockBuilder) finishBlock(fid uint64, checksumType byte) {
	defer b.resetBlockBuffer()
	b.blockKeys.append(b.block.tmpKeys.getEntry(0))
	b.blockAddrs = append(b.blockAddrs, newBlockAddress(fid, uint32(len(b.buf))))
	b.buf = AppendU32(b.buf, 0) // checksum place holder.
	beginOff := len(b.buf)
	numEntries := b.block.tmpKeys.length()
	b.buf = AppendU32(b.buf, uint32(numEntries))
	commonPrefix := b.getBlockCommonPrefix()
	offset := uint32(0)
	for i := 0; i < numEntries; i++ {
		b.buf = AppendU32(b.buf, offset)
		// The entry size calculated in the first pass use full key size, we need to subtract common prefix size.
		offset += b.block.entrySizes[i] - uint32(len(commonPrefix))
	}
	b.buf = AppendU16(b.buf, uint16(len(commonPrefix)))
	b.buf = append(b.buf, commonPrefix...)
	for i := 0; i < numEntries; i++ {
		b.buildEntry(i, len(commonPrefix))
	}
	checksum := uint32(0)
	if checksumType == crc32Castagnoli {
		checksum = crc32.Checksum(b.buf[beginOff:], crc32.MakeTable(crc32.Castagnoli))
	}
	binary.LittleEndian.PutUint32(b.buf[beginOff-4:], checksum)
}

func (b *blockBuilder) buildEntry(i, commonPrefixLen int) {
	key := b.block.tmpKeys.getEntry(i)
	keySuffix := key[commonPrefixLen:]
	b.buf = AppendU16(b.buf, uint16(len(keySuffix)))
	b.buf = append(b.buf, keySuffix...)
	valBin := b.block.tmpVals.getEntry(i)
	var val y.ValueStruct
	val.Decode(valBin)
	oldVersion := b.block.oldVers[i]
	if oldVersion != 0 {
		val.Meta |= metaHasOld
	}
	b.buf = append(b.buf, val.Meta)
	b.buf = AppendU64(b.buf, val.Version)
	if oldVersion != 0 {
		b.buf = AppendU64(b.buf, oldVersion)
	}
	b.buf = append(b.buf, byte(len(val.UserMeta)))
	b.buf = append(b.buf, val.UserMeta...)
	b.buf = append(b.buf, val.Value...)
}

func (b *blockBuilder) getBlockCommonPrefix() []byte {
	firstKey := b.block.tmpKeys.getEntry(0)
	lastKey := b.block.tmpKeys.getLast()
	return firstKey[:keyDiffIdx(firstKey, lastKey)]
}

func (b *blockBuilder) getIndexCommonPrefix() []byte {
	firstKey := b.blockKeys.getEntry(0)
	lastKey := b.blockKeys.getLast()
	return firstKey[:keyDiffIdx(firstKey, lastKey)]
}

func (b *blockBuilder) resetBlockBuffer() {
	b.block.tmpKeys.reset()
	b.block.tmpVals.reset()
	b.block.oldVers = b.block.oldVers[:0]
	b.block.entrySizes = b.block.entrySizes[:0]
	b.block.size = 0
}

func (b *blockBuilder) resetAll() {
	b.resetBlockBuffer()
	b.buf = b.buf[:0]
	b.blockAddrs = b.blockAddrs[:0]
	b.blockKeys.reset()
}

func (b *blockBuilder) buildIndex(baseOffset uint32, keyHashes []uint64, bloomFpr float64, checksumType byte) {
	// At this time the block data is already written to the writer, we can reuse the buffer to build index.
	b.buf = b.buf[:0]
	numBlocks := len(b.blockAddrs)
	// checksum place holder.
	b.buf = AppendU32(b.buf, 0)
	b.buf = AppendU32(b.buf, uint32(numBlocks))
	var commonPrefix []byte
	if numBlocks > 0 {
		commonPrefix = b.getIndexCommonPrefix()
	}
	commonPrefixLen := len(commonPrefix)
	keyOffset := 0
	for i := 0; i < numBlocks; i++ {
		b.buf = AppendU32(b.buf, uint32(keyOffset))
		blockKey := b.blockKeys.getEntry(i)
		keyOffset += len(blockKey) - commonPrefixLen
	}
	for i := 0; i < numBlocks; i++ {
		blockAddr := b.blockAddrs[i]
		b.buf = AppendU64(b.buf, blockAddr.originFID)
		b.buf = AppendU32(b.buf, blockAddr.originOff+baseOffset)
		b.buf = AppendU32(b.buf, blockAddr.currentOff+baseOffset)
	}
	b.buf = AppendU16(b.buf, uint16(commonPrefixLen))
	b.buf = append(b.buf, commonPrefix...)
	blockKeysLen := len(b.blockKeys.data) - numBlocks*commonPrefixLen
	b.buf = AppendU32(b.buf, uint32(blockKeysLen))
	for i := 0; i < numBlocks; i++ {
		blockKey := b.blockKeys.getEntry(i)
		b.buf = append(b.buf, blockKey[commonPrefixLen:]...)
	}
	if len(keyHashes) > 0 {
		b.buildBloomFilter(keyHashes, bloomFpr)
	}
	if checksumType == crc32Castagnoli {
		binary.LittleEndian.PutUint32(b.buf, crc32.Checksum(b.buf[4:], crc32.MakeTable(crc32.Castagnoli)))
	}
}

func (b *blockBuilder) buildBloomFilter(keyHashes []uint64, bloomFpr float64) {
	bf := bbloom.New(float64(len(keyHashes)), bloomFpr)
	for _, h := range keyHashes {
		bf.Add(h)
	}
	bloomData := bf.BinaryMarshal()
	b.buf = AppendU16(b.buf, bloomFilter)
	b.buf = AppendU32(b.buf, uint32(len(bloomData)))
	b.buf = append(b.buf, bloomData...)
}

func newBlockAddress(fid uint64, offset uint32) blockAddress {
	return blockAddress{
		originFID:  fid,
		originOff:  offset,
		currentOff: offset,
	}
}

// BuildResult contains the build result info, if it's file based compaction, fileName should be used to open Table.
// If it's in memory compaction, FileData and IndexData contains the data.
type BuildResult struct {
	FileName string
	FileData []byte
	Smallest []byte
	Biggest  []byte
}

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

func AppendU16(buf []byte, v uint16) []byte {
	return append(buf, byte(v), byte(v>>8))
}

func AppendU32(buf []byte, v uint32) []byte {
	return append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

func AppendU64(buf []byte, v uint64) []byte {
	return append(buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
		byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
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
