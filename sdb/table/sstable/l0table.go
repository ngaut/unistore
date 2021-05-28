package sstable

import (
	"bytes"
	"github.com/ngaut/unistore/sdb/table"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"reflect"
	"unsafe"
)

const l0FooterSize = int(unsafe.Sizeof(l0Footer{}))

type l0Footer struct {
	commitTS uint64
	numCFs   uint32
	magic    uint32
}

func (f *l0Footer) marshal() []byte {
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = l0FooterSize
	hdr.Cap = l0FooterSize
	hdr.Data = uintptr(unsafe.Pointer(f))
	return b
}

func (f *l0Footer) unmarshal(b []byte) {
	y.Assert(len(b) == l0FooterSize)
	*f = *(*l0Footer)(unsafe.Pointer(&b[0]))
}

type L0Table struct {
	l0Footer
	file     TableFile
	cfs      []*Table
	fid      uint64
	filename string
	cfOffs   []uint32
	smallest []byte
	biggest  []byte
}

func (st *L0Table) ID() uint64 {
	return st.fid
}

func (st *L0Table) Delete() error {
	st.file.Close()
	return st.file.Delete()
}

func (st *L0Table) GetCF(cf int) *Table {
	return st.cfs[cf]
}

func (st *L0Table) Size() int64 {
	return st.file.Size()
}

func (st *L0Table) Smallest() []byte {
	return st.smallest
}

func (st *L0Table) Biggest() []byte {
	return st.biggest
}

func (st *L0Table) CommitTS() uint64 {
	return st.commitTS
}

func OpenL0Table(filename string, fid uint64, smallest, biggest []byte) (*L0Table, error) {
	file, err := NewLocalFile(filename, true)
	if err != nil {
		return nil, err
	}
	l0 := &L0Table{
		fid:      fid,
		file:     file,
		filename: filename,
		smallest: smallest,
		biggest:  biggest,
	}
	footerOff := file.Size() - int64(l0FooterSize)
	l0.l0Footer.unmarshal(l0.file.MMapRead(footerOff, l0FooterSize))
	cfOffsOff := footerOff - 4*int64(l0.numCFs)
	l0.cfOffs = BytesToU32Slice(l0.file.MMapRead(cfOffsOff, 4*int(l0.numCFs)))
	l0.cfs = make([]*Table, 0, l0.numCFs)
	for i, off := range l0.cfOffs {
		endOff := uint32(cfOffsOff)
		if i+1 < len(l0.cfOffs) {
			endOff = l0.cfOffs[i+1]
		}
		data := l0.file.MMapRead(int64(off), int(endOff-off))
		if len(data) == 0 {
			l0.cfs = append(l0.cfs, nil)
			continue
		}
		inMemFile := NewInMemFile(fid, data)
		tbl, err := OpenTable(inMemFile, nil)
		if err != nil {
			return nil, err
		}
		l0.cfs = append(l0.cfs, tbl)
	}
	return l0, nil
}

func (sl0 *L0Table) Get(cf int, key []byte, version, keyHash uint64) y.ValueStruct {
	tbl := sl0.cfs[cf]
	if tbl == nil {
		return y.ValueStruct{}
	}
	v, err := tbl.Get(key, version, keyHash)
	if err != nil {
		// TODO: handle error
		log.Error("get data in table failed", zap.Error(err))
	}
	return v
}

func (sl0 *L0Table) NewIterator(cf int, reverse bool) table.Iterator {
	tbl := sl0.cfs[cf]
	if tbl == nil {
		return nil
	}
	return tbl.NewIterator(reverse)
}

func (sl0 *L0Table) Close() error {
	return sl0.file.Close()
}

type L0Builder struct {
	builders []*Builder
	commitTS uint64
}

func NewL0Builder(numCFs int, fid uint64, opt TableBuilderOptions, commitTS uint64) *L0Builder {
	sdb := &L0Builder{
		builders: make([]*Builder, numCFs),
		commitTS: commitTS,
	}
	for i := 0; i < numCFs; i++ {
		sdb.builders[i] = NewTableBuilder(fid, opt)
	}
	return sdb
}

func (e *L0Builder) Add(cf int, key []byte, value y.ValueStruct) {
	e.builders[cf].Add(key, &value)
}

func (e *L0Builder) Finish() []byte {
	cfDatas := make([][]byte, 0, len(e.builders)*2)
	estSize := 0
	for _, builder := range e.builders {
		estSize += builder.EstimateSize()
	}
	buffer := bytes.NewBuffer(make([]byte, 0, estSize))
	var offsets []uint32
	var fileSize int
	for _, builder := range e.builders {
		offsets = append(offsets, uint32(buffer.Len()))
		if builder.Empty() {
			continue
		}
		result, _ := builder.Finish("", buffer)
		cfDatas = append(cfDatas, result.FileData)
		fileSize += len(result.FileData)
	}
	buffer.Write(U32SliceToBytes(offsets))
	buf := buffer.Bytes()
	buf = AppendU64(buf, e.commitTS)
	buf = AppendU32(buf, uint32(len(e.builders)))
	buf = AppendU32(buf, MagicNumber)
	return buf
}

func (e *L0Builder) SmallestAndBiggest() (smallest, biggest []byte) {
	for i := 0; i < len(e.builders); i++ {
		builder := e.builders[i]
		if len(builder.smallest) > 0 {
			if len(smallest) == 0 || bytes.Compare(builder.smallest, smallest) < 0 {
				smallest = builder.smallest
			}
		}
		if bytes.Compare(builder.biggest, biggest) > 0 {
			biggest = builder.biggest
		}
	}
	y.Assert(len(smallest) > 0)
	y.Assert(len(biggest) > 0)
	return
}
