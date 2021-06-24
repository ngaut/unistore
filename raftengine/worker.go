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
	"fmt"
	"github.com/google/btree"
	"github.com/ngaut/unistore/engine/table/sstable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

type rotateTask struct {
	epochID uint32
	states  *btree.BTree
}

type worker struct {
	dir            string
	epochs         []*epoch
	truncatedIndex map[uint64]uint64
	buf            []byte
	taskCh         chan interface{}
	wg             sync.WaitGroup
	done           chan bool
}

func newWorker(dir string, epochs []*epoch) *worker {
	c := &worker{
		dir:            dir,
		epochs:         epochs,
		truncatedIndex: make(map[uint64]uint64),
		taskCh:         make(chan interface{}, 1024),
		done:           make(chan bool),
	}
	return c
}

func (c *worker) run() {
	defer c.wg.Done()
	for {
		select {
		case <-c.done:
			return
		case task := <-c.taskCh:
			switch x := task.(type) {
			case rotateTask:
				c.handleRotateTask(x)
			case truncateOp:
				c.handleTruncateTask(x)
			}
		}
	}
}

func (c *worker) handleRotateTask(task rotateTask) {
	err := c.writeStates(task.epochID, task.states)
	if err != nil {
		log.Error("failed to write states", zap.Error(err))
	}
	c.epochs = append(c.epochs, &epoch{
		id:            task.epochID,
		hasWALFile:    true,
		hasStatesFile: err == nil,
		raftLogFiles:  map[uint64]raftLogRange{},
	})
	if len(c.epochs) >= 2 {
		err = c.compact(c.epochs[len(c.epochs)-2])
		if err != nil {
			log.Error("failed to write states", zap.Error(err))
		}
	}
}

func (c *worker) writeStates(epochID uint32, states *btree.BTree) error {
	c.buf = c.buf[:0]
	states.Ascend(func(i btree.Item) bool {
		item := i.(*stateItem)
		c.buf = sstable.AppendU64(c.buf, item.regionID)
		c.buf = sstable.AppendU16(c.buf, uint16(len(item.key)))
		c.buf = append(c.buf, item.key...)
		c.buf = sstable.AppendU32(c.buf, uint32(len(item.val)))
		c.buf = append(c.buf, item.val...)
		return true
	})
	checksum := crc32.Checksum(c.buf, crc32.MakeTable(crc32.Castagnoli))
	c.buf = sstable.AppendU32(c.buf, checksum)
	filename := statesFileName(c.dir, epochID)
	fd, err := y.OpenTruncFile(filename, false)
	if err != nil {
		return err
	}
	_, err = fd.Write(c.buf)
	if err != nil {
		return err
	}
	err = fd.Close()
	if err != nil {
		return err
	}
	if epochID == 1 {
		return nil
	}
	oldEpochStateFile := statesFileName(c.dir, epochID-1)
	return os.Remove(oldEpochStateFile)
}

func (c *worker) compact(e *epoch) error {
	entriesMap := map[uint64]*regionRaftLogs{}
	it := newIterator(c.dir, e.id)
	err := it.iterate(func(tp uint32, entry []byte) (stop bool) {
		if tp != typeRaftLog {
			return
		}
		op := parseLog(y.Copy(entry))
		if c.truncatedIndex[op.regionID] > op.index {
			return
		}
		entries := getRegionRaftLogs(entriesMap, op.regionID)
		entries.append(op)
		return
	})
	if err != nil {
		return err
	}
	log.S().Infof("epoch %d compact wal file generate %d files", e.id, len(entriesMap))
	for regionID, entries := range entriesMap {
		err = c.writeRaftLogFile(e, regionID, entries)
		if err != nil {
			return err
		}
	}

	return os.Rename(walFileName(c.dir, e.id), recycleFileName(c.dir, e.id))
}

func (c *worker) writeRaftLogFile(e *epoch, regionID uint64, entries *regionRaftLogs) error {
	filename := raftLogFileName(c.dir, e.id, regionID, entries.raftLogRange)
	fd, err := y.OpenTruncFile(filename, false)
	if err != nil {
		return err
	}
	defer fd.Close()
	c.buf = c.buf[:0]
	for _, op := range entries.raftLogs {
		c.buf = sstable.AppendU32(c.buf, uint32(8+len(op.data)))
		c.buf = sstable.AppendU32(c.buf, op.term)
		c.buf = sstable.AppendU32(c.buf, op.eType)
		c.buf = append(c.buf, op.data...)
	}
	checksum := crc32.Checksum(c.buf, crc32.MakeTable(crc32.Castagnoli))
	c.buf = sstable.AppendU32(c.buf, checksum)
	_, err = fd.Write(c.buf)
	if err != nil {
		return err
	}
	err = fd.Sync()
	if err != nil {
		return err
	}
	e.raftLogFiles[regionID] = entries.raftLogRange
	return nil
}

func (c *worker) handleTruncateTask(task truncateOp) {
	c.truncatedIndex[task.regionID] = task.index
	var toBeRemoved []*epoch
	for _, epoch := range c.epochs {
		rRan, ok := epoch.raftLogFiles[task.regionID]
		if ok && rRan.endIndex <= task.index {
			filename := raftLogFileName(c.dir, epoch.id, task.regionID, rRan)
			err := os.Remove(filename)
			if err != nil {
				log.Error("failed to remove rlog file", zap.String("filename", filename), zap.Error(err))
			} else {
				log.S().Infof("remove file %s", filename)
			}
			delete(epoch.raftLogFiles, task.regionID)
			if len(epoch.raftLogFiles) == 0 {
				toBeRemoved = append(toBeRemoved, epoch)
			}
		} else if ok {
			log.S().Infof("rlog file not deletable endIdx %d, truncateIdx %d", rRan.endIndex, task.index)
		}
	}
	for len(toBeRemoved) > 0 {
		target := toBeRemoved[0]
		toBeRemoved = toBeRemoved[1:]
		for i, ep := range c.epochs {
			if ep == target {
				c.epochs = append(c.epochs[:i], c.epochs[i+1:]...)
				break
			}
		}
	}
}

func raftLogFileName(dir string, epochID uint32, regionID uint64, rlogRange raftLogRange) string {
	return filepath.Join(dir, fmt.Sprintf("%08x_%016x_%016x_%016x.rlog",
		epochID, regionID, rlogRange.startIndex, rlogRange.endIndex))
}

func statesFileName(dir string, epochID uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%08x.states", epochID))
}

func walFileName(dir string, epochID uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%08x.wal", epochID))
}

func recycleFileName(dir string, epochID uint32) string {
	return filepath.Join(dir, "recycle", fmt.Sprintf("%08x.wal", epochID))
}
