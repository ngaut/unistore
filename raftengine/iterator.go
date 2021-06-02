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
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

type walIterator struct {
	dir         string
	epochID     uint32
	buf         []byte
	batchHdrBuf [batchHdrSize]byte
	offset      int64
}

func newIterator(dir string, epochID uint32) *walIterator {
	return &walIterator{
		dir:     dir,
		epochID: epochID,
	}
}

func (it *walIterator) iterate(fn func(tp uint32, entry []byte) (stop bool)) error {
	filename := walFileName(it.dir, it.epochID)
	fd, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fd.Close()
	bufReader := bufio.NewReaderSize(fd, 256*1024)
	for {
		batch, err1 := it.readBatch(bufReader)
		if err1 != nil {
			if err1 == io.EOF {
				return nil
			}
			return err1
		}
		if len(batch) == 0 {
			return nil
		}
		it.offset += 12 + int64(len(batch))
		for len(batch) > 0 {
			tp := binary.LittleEndian.Uint32(batch)
			batch = batch[4:]
			length := binary.LittleEndian.Uint32(batch)
			batch = batch[4:]
			entry := batch[:length]
			batch = batch[length:]
			if fn(tp, entry) {
				return nil
			}
		}
	}
}

func (it *walIterator) readBatch(reader io.Reader) ([]byte, error) {
	_, err := io.ReadFull(reader, it.batchHdrBuf[:])
	if err != nil {
		return nil, err
	}
	epochID := binary.LittleEndian.Uint32(it.batchHdrBuf[:])
	if epochID != it.epochID {
		return nil, io.EOF
	}
	checksum := binary.LittleEndian.Uint32(it.batchHdrBuf[4:])
	length := binary.LittleEndian.Uint32(it.batchHdrBuf[8:])
	if cap(it.buf) >= int(length) {
		it.buf = it.buf[:length]
	} else {
		it.buf = make([]byte, length, length+length/2)
	}
	_, err = io.ReadFull(reader, it.buf)
	if err != nil {
		return nil, err
	}
	if crc32.Checksum(it.buf, crc32.MakeTable(crc32.Castagnoli)) != checksum {
		return nil, io.EOF
	}
	return it.buf, nil
}

func parseState(entry []byte) (regionID uint64, key, val []byte) {
	regionID = binary.LittleEndian.Uint64(entry)
	entry = entry[8:]
	keyLen := binary.LittleEndian.Uint16(entry)
	entry = entry[2:]
	key = entry[:keyLen]
	val = entry[keyLen:]
	return
}

func parseLog(entry []byte) (regionID, index uint64, rlog []byte) {
	regionID = binary.LittleEndian.Uint64(entry)
	entry = entry[8:]
	index = binary.LittleEndian.Uint64(entry)
	entry = entry[8:]
	rlog = entry
	return
}

func parseTruncate(entry []byte) (regionID, index uint64) {
	regionID = binary.LittleEndian.Uint64(entry)
	entry = entry[8:]
	index = binary.LittleEndian.Uint64(entry)
	return
}
