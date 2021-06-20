// Copyright 2021-present PingCAP, Inc.
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

package raftengine

import (
	"encoding/binary"
	"github.com/ncw/directio"
	"github.com/ngaut/unistore/engine/table/sstable"
	"github.com/pingcap/errors"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

const (
	entryHdrSize = 8
	batchHdrSize = 12
	alignSize    = 4096
	alignMask    = 0xfffff000

	// make it greater than max raft log, so a reallocation will always have sufficient size.
	initialBufSize = 8 * 1024 * 1024
	recycleDir     = "recycle"
)

type walWriter struct {
	dir         string
	epochID     uint32
	walSize     int64
	fd          *os.File
	entryHdrBuf [entryHdrSize]byte
	buf         []byte

	// bufBatchOff is not aligned.
	bufBatchOff int64

	// fileOff is always aligned.
	fileOff int64
}

func newWalWriter(dir string, epochID uint32, offset, walSize int64) (*walWriter, error) {
	walSize = (walSize + alignSize - 1) & alignMask
	writer := &walWriter{
		dir:     dir,
		epochID: epochID,
		walSize: walSize,
		buf:     directio.AlignedBlock(initialBufSize),
	}
	err := writer.openFile()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if offset != 0 {
		err = writer.seek(offset)
		if err != nil {
			return nil, err
		}
	}
	return writer, nil
}

func (w *walWriter) seek(offset int64) error {
	w.fileOff = offset & alignMask
	if w.fileOff != offset {
		_, err := w.fd.ReadAt(w.buf[:alignSize], w.fileOff)
		if err != nil {
			return err
		}
		w.bufBatchOff = offset - w.fileOff
		w.buf = w.buf[:w.bufBatchOff+batchHdrSize]
	}
	return nil
}

func (w *walWriter) offset() int64 {
	return w.fileOff + w.bufBatchOff
}

func (w *walWriter) reallocate() {
	newBuf := directio.AlignedBlock(cap(w.buf) * 2)
	copy(newBuf, w.buf)
	newBuf = newBuf[:len(w.buf)]
	w.buf = newBuf
}

func (w *walWriter) alignedBufLen() int {
	return (len(w.buf) + alignSize - 1) & alignMask
}

func (w *walWriter) appendHdr(tp uint32, length int) {
	binary.LittleEndian.PutUint32(w.entryHdrBuf[:], tp)
	binary.LittleEndian.PutUint32(w.entryHdrBuf[4:], uint32(length))
	w.buf = append(w.buf, w.entryHdrBuf[:]...)
}

func raftLogSize(data []byte) int {
	return entryHdrSize + 16 + len(data)
}

func (w *walWriter) appendRaftLog(regionID, index uint64, data []byte) {
	size := raftLogSize(data)
	if len(w.buf)+size > cap(w.buf) {
		w.reallocate()
	}
	w.appendHdr(typeRaftLog, size-entryHdrSize)
	w.buf = sstable.AppendU64(w.buf, regionID)
	w.buf = sstable.AppendU64(w.buf, index)
	w.buf = append(w.buf, data...)
}

func stateSize(key, val []byte) int {
	return entryHdrSize + 8 + 2 + len(key) + len(val)
}

func (w *walWriter) appendState(regionID uint64, key, val []byte) {
	size := stateSize(key, val)
	if size+len(w.buf) > cap(w.buf) {
		w.reallocate()
	}
	w.appendHdr(typeState, size-entryHdrSize)
	w.buf = sstable.AppendU64(w.buf, regionID)
	w.buf = sstable.AppendU16(w.buf, uint16(len(key)))
	w.buf = append(w.buf, key...)
	w.buf = append(w.buf, val...)
}

func truncateSize() int {
	return entryHdrSize + 16
}

func (w *walWriter) appendTruncate(regionID, index uint64) {
	size := truncateSize()
	if size+len(w.buf) > cap(w.buf) {
		w.reallocate()
	}
	w.appendHdr(typeTruncate, 16)
	w.buf = sstable.AppendU64(w.buf, regionID)
	w.buf = sstable.AppendU64(w.buf, index)
}

func (w *walWriter) flush() error {
	batch := w.buf[w.bufBatchOff:]
	batchHdr := batch[:batchHdrSize]
	batchPayload := batch[batchHdrSize:]
	binary.LittleEndian.PutUint32(batchHdr, w.epochID)
	checksum := crc32.Checksum(batchPayload, crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(batchHdr[4:], checksum)
	binary.LittleEndian.PutUint32(batchHdr[8:], uint32(len(batchPayload)))
	_, err := w.fd.WriteAt(w.buf[:w.alignedBufLen()], w.fileOff)
	if err != nil {
		return err
	}
	w.fileOff += int64(w.alignedBufLen())
	if len(w.buf) != w.alignedBufLen() {
		leftOverData := w.buf[w.alignedBufLen()-alignSize:]
		w.buf = append(w.buf[:0], leftOverData...)
		w.fileOff -= alignSize
	} else {
		w.buf = w.buf[:0]
	}
	w.bufBatchOff = int64(len(w.buf))
	w.reserveBatchHdr()
	return nil
}

func (w *walWriter) reserveBatchHdr() {
	w.buf = w.buf[:len(w.buf)+batchHdrSize]
}

func (w *walWriter) rotate() error {
	err := w.fd.Close()
	if err != nil {
		return err
	}
	w.epochID++
	return w.openFile()
}

func (w *walWriter) openFile() error {
	filename := walFileName(w.dir, w.epochID)
	openFlags := os.O_RDWR | syscall.O_DSYNC
	if !fileExists(filename) {
		recycledFilename := w.findRecycledFile()
		if recycledFilename != "" {
			err := os.Rename(recycledFilename, filename)
			if err != nil {
				return err
			}
		} else {
			openFlags |= os.O_CREATE
		}
	}
	var err error
	w.fd, err = directio.OpenFile(filename, openFlags, 0666)
	if err != nil {
		return err
	}
	err = w.fd.Truncate(w.walSize)
	if err != nil {
		return err
	}
	w.buf = w.buf[:batchHdrSize]
	w.bufBatchOff = 0
	w.fileOff = 0
	return nil
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func (w *walWriter) findRecycledFile() string {
	var filename string
	err := filepath.Walk(filepath.Join(w.dir, recycleDir), func(path string, info fs.FileInfo, err error) error {
		if info != nil && info.IsDir() {
			return nil
		}
		filename = path
		return filepath.SkipDir
	})
	if err != nil {
		return ""
	}
	return filename
}
