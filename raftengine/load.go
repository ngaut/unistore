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
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"hash/crc32"
	"io/fs"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	epochLen         = 8
	regionIDLen      = 16
	startIndexLen    = 16
	endIndexLen      = 16
	regionIDOffset   = epochLen + 1
	startIndexOffset = regionIDOffset + 1 + regionIDLen
	endIndexOffset   = startIndexOffset + 1 + startIndexLen
)

type epoch struct {
	id            uint32
	hasStatesFile bool
	hasWALFile    bool
	raftLogFiles  map[uint64]raftLogRange
}

type raftLogRange struct {
	startIndex uint64
	endIndex   uint64
}

func getEpoch(epochs map[uint32]*epoch, epochID uint32) *epoch {
	ep, ok := epochs[epochID]
	if !ok {
		ep = &epoch{id: epochID, raftLogFiles: map[uint64]raftLogRange{}}
		epochs[epochID] = ep
	}
	return ep
}

func (e *epoch) addFile(filename string) error {
	if strings.HasSuffix(filename, "wal") {
		e.hasWALFile = true
	} else if strings.HasSuffix(filename, "states") {
		e.hasStatesFile = true
	} else if strings.HasSuffix(filename, "rlog") {
		regionID, err := strconv.ParseUint(filename[regionIDOffset:regionIDOffset+regionIDLen], 16, 64)
		if err != nil {
			return err
		}
		startIndex, err := strconv.ParseUint(filename[startIndexOffset:startIndexOffset+startIndexLen], 16, 64)
		if err != nil {
			return err
		}
		endIndex, err := strconv.ParseUint(filename[endIndexOffset:endIndexOffset+endIndexLen], 16, 64)
		if err != nil {
			return err
		}
		e.raftLogFiles[regionID] = raftLogRange{startIndex: startIndex, endIndex: endIndex}
	}
	return nil
}

func (e *Engine) readEpochs() ([]*epoch, error) {
	epochMap := make(map[uint32]*epoch)
	recyclePath := filepath.Join(e.dir, recycleDir)
	err := filepath.Walk(e.dir, func(path string, info fs.FileInfo, err error) error {
		if path == e.dir || strings.HasPrefix(path, recyclePath) {
			return nil
		}
		_, filename := filepath.Split(path)
		epochID, err := strconv.ParseUint(filename[:8], 16, 32)
		if err != nil {
			return err
		}
		ep := getEpoch(epochMap, uint32(epochID))
		return ep.addFile(filename)
	})
	if err != nil {
		return nil, err
	}
	epochs := make([]*epoch, 0, len(epochMap))
	for _, ep := range epochMap {
		epochs = append(epochs, ep)
	}
	sort.Slice(epochs, func(i, j int) bool {
		return epochs[i].id < epochs[j].id
	})
	return epochs, nil
}

func (e *Engine) loadEpoch(ep *epoch) (walOffset int64, err error) {
	log.S().Infof("load epoch %d, rlog files %d, hasWAL %v, hasState %v", ep.id, len(ep.raftLogFiles), ep.hasWALFile, ep.hasStatesFile)
	if ep.hasWALFile {
		walOffset, err = e.loadWALFile(ep.id)
		if err != nil {
			return 0, err
		}
	} else {
		for regionID, rlogRange := range ep.raftLogFiles {
			err = e.loadRaftLogFile(ep.id, regionID, rlogRange)
			if err != nil {
				return 0, err
			}
		}
	}
	if ep.hasStatesFile {
		err = e.loadStateFile(ep.id)
		if err != nil {
			return 0, err
		}
	}
	return
}

const (
	typeState    uint32 = 1
	typeRaftLog  uint32 = 2
	typeTruncate uint32 = 3
)

func (e *Engine) loadWALFile(epochID uint32) (offset int64, err error) {
	it := newIterator(e.dir, epochID)
	err = it.iterate(func(tp uint32, entryData []byte) bool {
		switch tp {
		case typeState:
			regionID, key, val := parseState(y.Copy(entryData))
			e.states.ReplaceOrInsert(&stateItem{regionID: regionID, key: key, val: val})
		case typeRaftLog:
			logOp := parseLog(y.Copy(entryData))
			entries := getRegionRaftLogs(e.entriesMap, logOp.regionID)
			entries.append(logOp)
		case typeTruncate:
			regionID, index := parseTruncate(entryData)
			entries := getRegionRaftLogs(e.entriesMap, regionID)
			if empty := entries.truncate(index); empty {
				delete(e.entriesMap, regionID)
			}
		}
		return false
	})
	return it.offset, err
}

func (e *Engine) loadStateFile(epochID uint32) error {
	filename := statesFileName(e.dir, epochID)
	data, err := readFile(filename)
	for len(data) > 0 {
		regionID := binary.LittleEndian.Uint64(data)
		data = data[8:]
		keyLen := binary.LittleEndian.Uint16(data)
		data = data[2:]
		key := data[:keyLen]
		data = data[keyLen:]
		valLen := binary.LittleEndian.Uint32(data)
		data = data[4:]
		val := data[:valLen]
		data = data[valLen:]
		e.states.ReplaceOrInsert(&stateItem{regionID: regionID, key: y.Copy(key), val: y.Copy(val)})
	}
	return err
}

func readFile(filename string) ([]byte, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	checksumOff := len(data) - 4
	checksum := binary.LittleEndian.Uint32(data[checksumOff:])
	if got := crc32.Checksum(data[:checksumOff], crc32.MakeTable(crc32.Castagnoli)); got != checksum {
		return nil, errors.Errorf("checksum failed expect %d got %d", checksum, got)
	}
	return data[:checksumOff], nil
}

func (e *Engine) loadRaftLogFile(epochID uint32, regionID uint64, raftLogRange raftLogRange) error {
	rlogFilename := raftLogFileName(e.dir, epochID, regionID, raftLogRange)
	data, err := readFile(rlogFilename)
	if err != nil {
		return err
	}
	index := raftLogRange.startIndex
	for len(data) > 0 {
		length := binary.LittleEndian.Uint32(data)
		data = data[4:]
		entry := data[:length]
		data = data[length:]
		var op raftLogOp
		op.regionID = regionID
		op.index = index
		op.term = binary.LittleEndian.Uint32(entry)
		entry = entry[4:]
		op.eType = binary.LittleEndian.Uint32(entry)
		entry = entry[4:]
		op.data = entry
		entries := getRegionRaftLogs(e.entriesMap, regionID)
		entries.append(op)
		index++
	}
	y.Assert(index == raftLogRange.endIndex)
	return nil
}

func (e *Engine) loadTruncate(entry []byte) {
	regionID, index := parseTruncate(entry)
	entries := getRegionRaftLogs(e.entriesMap, regionID)
	if empty := entries.truncate(index); empty {
		delete(e.entriesMap, regionID)
	}
}
