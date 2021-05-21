package sstable

import (
	"github.com/ngaut/unistore/sdb/cache"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"io"
	"os"
)

type TableFile interface {
	ID() uint64
	Size() int64
	ReadAt(b []byte, off int64) (n int, err error)
	IsMMap() bool
	MMapRead(off int64, length int) []byte
	LoadToRam() error
	Close() error
	Delete() error
}

// InMemFile keep data in memory.
type InMemFile struct {
	id   uint64
	data []byte
}

func (f *InMemFile) ID() uint64 {
	return f.id
}

func (f *InMemFile) Size() int64 {
	return int64(len(f.data))
}

func (f *InMemFile) ReadAt(b []byte, off int64) (n int, err error) {
	if off >= f.Size() {
		return 0, io.EOF
	}
	n = copy(b, f.data[off:])
	if n < len(b) {
		err = io.EOF
	}
	return
}

func (f *InMemFile) IsMMap() bool {
	return true
}

func (f *InMemFile) MMapRead(off int64, length int) []byte {
	return f.data[off : off+int64(length)]
}

func (f *InMemFile) LoadToRam() error {
	return nil
}

func (f *InMemFile) Close() error {
	return nil
}

func (f *InMemFile) Delete() error {
	return nil
}

func NewInMemFile(id uint64, data []byte) *InMemFile {
	return &InMemFile{id: id, data: data}
}

func getFdWithSize(filename string) (fd *os.File, size int64, err error) {
	fd, err = y.OpenExistingFile(filename, 0)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	fdi, err := fd.Stat()
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	return fd, fdi.Size(), nil
}

type LocalFile struct {
	filename string
	fid      uint64
	fd       *os.File
	size     int64
	mmapData []byte
}

func NewLocalFile(filename string, mmap bool) (*LocalFile, error) {
	fid, ok := ParseFileID(filename)
	if !ok {
		return nil, errors.Errorf("Invalid filename: %s", filename)
	}
	fd, tblSize, err := getFdWithSize(filename)
	if err != nil {
		return nil, err
	}
	nFile := &LocalFile{
		fid:  fid,
		fd:   fd,
		size: tblSize,
	}
	if mmap {
		nFile.mmapData, err = y.Mmap(fd, false, tblSize)
		if err != nil {
			fd.Close()
			return nil, y.Wrapf(err, "Unable to map file")
		}
	}
	return nFile, nil
}

func (f *LocalFile) ID() uint64 {
	return f.fid
}

func (f *LocalFile) Size() int64 {
	return f.size
}

func (f *LocalFile) ReadAt(b []byte, off int64) (n int, err error) {
	return f.fd.ReadAt(b, off)
}

func (f *LocalFile) IsMMap() bool {
	return f.mmapData != nil
}

func (f *LocalFile) MMapRead(off int64, length int) []byte {
	return f.mmapData[off : off+int64(length)]
}

func (f *LocalFile) LoadToRam() error {
	return nil
}

func (f *LocalFile) Close() error {
	if f.mmapData != nil {
		err := y.Munmap(f.mmapData)
		if err != nil {
			log.Error("munmap failed", zap.Error(err))
		}
		f.mmapData = nil
	}
	return f.fd.Close()
}

func (f *LocalFile) Delete() error {
	f.Close()
	return os.Remove(f.fd.Name())
}

func blockCacheKey(fid uint64, offset uint32) cache.Key {
	return cache.Key{ID: fid, Offset: offset}
}
