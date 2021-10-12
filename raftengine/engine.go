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
	"bytes"
	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"os"
	"path/filepath"
	"sync"
)

type Engine struct {
	dir        string
	writer     *walWriter
	writeMu    sync.Mutex
	entriesMap map[uint64]*RegionRaftLogs
	mapMu      sync.RWMutex
	states     *btree.BTree
	stateMu    sync.RWMutex
	worker     *worker
}

type WriteBatch struct {
	truncates  []truncateOp
	raftLogOps []raftLogOp
	stateOps   []stateOp
	size       int
}

func NewWriteBatch() *WriteBatch {
	return &WriteBatch{size: batchHdrSize}
}

func (b *WriteBatch) AppendRaftLog(regionID uint64, entry *eraftpb.Entry) {
	op := newRaftLogOp(regionID, entry)
	b.raftLogOps = append(b.raftLogOps, op)
	b.size += raftLogSize(op)
}

func (b *WriteBatch) TruncateRaftLog(regionID, index uint64) {
	b.truncates = append(b.truncates, truncateOp{regionID: regionID, index: index})
	b.size += truncateSize()
}

func (b *WriteBatch) SetState(regionID uint64, key, val []byte) {
	b.stateOps = append(b.stateOps, stateOp{regionID: regionID, key: key, val: val})
	b.size += stateSize(key, val)
}

func (wb *WriteBatch) Size() int {
	return wb.size
}

func (wb *WriteBatch) NumEntries() int {
	return len(wb.truncates) + len(wb.raftLogOps) + len(wb.stateOps)
}

func (b *WriteBatch) Reset() {
	b.raftLogOps = b.raftLogOps[:0]
	b.truncates = b.truncates[:0]
	b.stateOps = b.stateOps[:0]
	b.size = batchHdrSize
}

func (b *WriteBatch) IsEmpty() bool {
	return b.size == batchHdrSize
}

type truncateOp struct {
	regionID uint64
	index    uint64
}

type raftLogOp struct {
	regionID uint64
	index    uint64
	term     uint32
	eType    uint32
	data     []byte
}

func newRaftLogOp(regionID uint64, entry *eraftpb.Entry) raftLogOp {
	return raftLogOp{
		regionID: regionID,
		index:    entry.Index,
		term:     uint32(entry.Term),
		eType:    uint32(entry.EntryType),
		data:     entry.Data,
	}
}

type stateOp struct {
	regionID uint64
	key      []byte
	val      []byte
}

func (e *Engine) Write(wb *WriteBatch) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	if int64(wb.size)+e.writer.offset() > e.writer.walSize {
		epochID := e.writer.epochID
		err := e.writer.rotate()
		if err != nil {
			return err
		}
		e.stateMu.Lock()
		states := e.states
		e.states = btree.New(8)
		states.Ascend(func(i btree.Item) bool {
			e.states.ReplaceOrInsert(i)
			return true
		})
		e.stateMu.Unlock()
		e.worker.taskCh <- rotateTask{epochID: epochID, states: states}
	}
	for _, op := range wb.raftLogOps {
		e.writer.appendRaftLog(op)
	}
	var regionRaftLogs *RegionRaftLogs
	var lastRegionID uint64
	for _, op := range wb.raftLogOps {
		if regionRaftLogs == nil || op.regionID != lastRegionID {
			regionRaftLogs = e.GetRegionRaftLogs(op.regionID)
			lastRegionID = op.regionID
		}
		regionRaftLogs.append(op)
	}
	for _, op := range wb.stateOps {
		e.writer.appendState(op.regionID, op.key, op.val)
	}
	e.stateMu.Lock()
	for _, op := range wb.stateOps {
		item := &stateItem{regionID: op.regionID, key: op.key, val: op.val}
		if len(op.val) > 0 {
			e.states.ReplaceOrInsert(item)
		} else {
			e.states.Delete(item)
		}
	}
	e.stateMu.Unlock()
	for _, op := range wb.truncates {
		e.writer.appendTruncate(op.regionID, op.index)
		e.mapMu.Lock()
		if empty := getRegionRaftLogs(e.entriesMap, op.regionID).truncate(op.index); empty {
			delete(e.entriesMap, op.regionID)
		}
		e.mapMu.Unlock()
		e.worker.taskCh <- truncateOp{regionID: op.regionID, index: op.index}
	}
	return e.writer.flush()
}

func (e *Engine) IsEmpty() bool {
	e.mapMu.RLock()
	mapEmpty := len(e.entriesMap) == 0
	e.mapMu.RUnlock()
	e.stateMu.RLock()
	statesEmpty := e.states.Len() == 0
	e.stateMu.RUnlock()
	return mapEmpty && statesEmpty
}

func getRegionRaftLogs(entriesMap map[uint64]*RegionRaftLogs, regionID uint64) *RegionRaftLogs {
	re, ok := entriesMap[regionID]
	if !ok {
		re = &RegionRaftLogs{}
		entriesMap[regionID] = re
	}
	return re
}

type stateItem struct {
	regionID uint64
	key      []byte
	val      []byte
}

func (b *stateItem) Less(than btree.Item) bool {
	thanItem := than.(*stateItem)
	if b.regionID != thanItem.regionID {
		return b.regionID < thanItem.regionID
	}
	return bytes.Compare(b.key, than.(*stateItem).key) < 0
}

func (e *Engine) GetState(regionID uint64, key []byte) []byte {
	e.stateMu.RLock()
	val := e.states.Get(&stateItem{regionID: regionID, key: key})
	e.stateMu.RUnlock()
	if val != nil {
		return val.(*stateItem).val
	}
	return nil
}

type RegionRaftLogs struct {
	raftLogRange
	raftLogs []raftLogOp
}

func (re *RegionRaftLogs) prepareAppend(index uint64) {
	if re.startIndex == 0 {
		// initialize index
		re.startIndex = index
		re.endIndex = index
		return
	}
	if index < re.startIndex || re.endIndex < index {
		// Out of bound index truncate all entries.
		re.startIndex = index
		re.endIndex = index
		re.raftLogs = re.raftLogs[:0]
		return
	}
	localIdx := index - re.startIndex
	re.raftLogs = re.raftLogs[:localIdx]
	re.endIndex = index
}

func (re *RegionRaftLogs) append(op raftLogOp) {
	re.prepareAppend(op.index)
	re.raftLogs = append(re.raftLogs, op)
	re.endIndex++
}

func (re *RegionRaftLogs) truncate(index uint64) (empty bool) {
	if index <= re.startIndex {
		return
	}
	if index > re.endIndex {
		re.startIndex = index
		re.endIndex = index
		re.raftLogs = re.raftLogs[:0]
		return true
	}
	localIdx := index - re.startIndex
	re.startIndex = index
	re.raftLogs = re.raftLogs[localIdx:]
	return len(re.raftLogs) == 0
}

func (re *RegionRaftLogs) Get(index uint64) *eraftpb.Entry {
	if index < re.startIndex || index >= re.endIndex {
		return nil
	}
	localIdx := index - re.startIndex
	op := re.raftLogs[localIdx]
	return &eraftpb.Entry{
		EntryType: eraftpb.EntryType(op.eType),
		Term:      uint64(op.term),
		Index:     op.index,
		Data:      op.data,
	}
}

func (re *RegionRaftLogs) GetRange() (startIndex, endIndex uint64) {
	return re.startIndex, re.endIndex
}

func (e *Engine) GetRegionRaftLogs(regionID uint64) *RegionRaftLogs {
	e.mapMu.RLock()
	re, ok := e.entriesMap[regionID]
	e.mapMu.RUnlock()
	if ok {
		return re
	}
	e.mapMu.Lock()
	defer e.mapMu.Unlock()
	re, ok = e.entriesMap[regionID]
	if !ok {
		re = &RegionRaftLogs{}
		e.entriesMap[regionID] = re
	}
	return re
}

func (e *Engine) IterateRegionStates(regionID uint64, desc bool, fn func(key, val []byte) error) error {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	var err error
	startItem := &stateItem{regionID: regionID}
	endItem := &stateItem{regionID: regionID + 1}
	iterator := func(i btree.Item) bool {
		item := i.(*stateItem)
		err = fn(item.key, item.val)
		return err == nil
	}
	if desc {
		e.states.DescendRange(endItem, startItem, iterator)
	} else {
		e.states.AscendRange(startItem, endItem, iterator)
	}
	return err
}

func (e *Engine) IterateAllStates(desc bool, fn func(regionID uint64, key, val []byte) error) error {
	e.stateMu.RLock()
	defer e.stateMu.RUnlock()
	var err error
	iterator := func(i btree.Item) bool {
		item := i.(*stateItem)
		err = fn(item.regionID, item.key, item.val)
		return err == nil
	}
	if desc {
		e.states.Descend(iterator)
	} else {
		e.states.Ascend(iterator)
	}
	return err
}

func Open(dir string, walSize int64) (*Engine, error) {
	err := maybeCreateDir(dir)
	if err != nil {
		return nil, err
	}
	e := &Engine{
		dir:        dir,
		entriesMap: map[uint64]*RegionRaftLogs{},
		states:     btree.New(8),
	}
	epochSlice, err := e.readEpochs()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var epochID uint32
	var offset int64
	for _, ep := range epochSlice {
		epochID = ep.id
		offset, err = e.loadEpoch(ep)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if epochID == 0 {
		epochID = 1
	}
	e.writer, err = newWalWriter(dir, epochID, offset, walSize)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(epochSlice) > 0 {
		// The last epoch is writable, exclude it for the worker.
		epochSlice = epochSlice[:len(epochSlice)-1]
	}
	e.worker = newWorker(dir, epochSlice)
	e.worker.wg.Add(1)
	go e.worker.run()
	return e, nil
}

func maybeCreateDir(dir string) error {
	recyclePath := filepath.Join(dir, recycleDir)
	fi, err := os.Stat(recyclePath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(recyclePath, 0700)
	}
	if err != nil {
		return err
	}
	if fi != nil && !fi.IsDir() {
		return errors.New("recycle path is not dir")
	}
	return nil
}

func (e *Engine) Close() error {
	close(e.worker.done)
	e.worker.wg.Wait()
	return e.writer.fd.Close()
}
