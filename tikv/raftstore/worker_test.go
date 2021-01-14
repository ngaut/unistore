// Copyright 2019-present PingCAP, Inc.
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

package raftstore

import (
	"testing"
	"time"

	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/stretchr/testify/assert"
)

func TestStalePeerInfo(t *testing.T) {
	timeout := time.Now()
	regionId := uint64(1)
	endKey := []byte{1, 2, 10, 10, 101, 10, 1, 9}
	peerInfo := newStalePeerInfo(regionId, endKey, timeout)
	assert.Equal(t, peerInfo.regionId(), regionId)
	assert.Equal(t, peerInfo.endKey(), endKey)
	assert.True(t, peerInfo.timeout().Equal(timeout))

	regionId = 101
	endKey = []byte{102, 11, 98, 23, 45, 76, 14, 43}
	timeout = time.Now().Add(time.Millisecond * 101)
	peerInfo.setRegionId(regionId)
	peerInfo.setEndKey(endKey)
	peerInfo.setTimeout(timeout)
	assert.Equal(t, peerInfo.regionId(), regionId)
	assert.Equal(t, peerInfo.endKey(), endKey)
	assert.True(t, peerInfo.timeout().Equal(timeout))
}

func TestGcRaftLog(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)
	raftDb := engines.raft
	taskResCh := make(chan raftLogGcTaskRes, 1)
	runner := raftLogGCTaskHandler{taskResCh: taskResCh}

	//  generate raft logs
	regionId := uint64(1)
	raftWb := new(RaftWriteBatch)
	for i := uint64(0); i < 100; i++ {
		k := RaftLogKey(regionId, i)
		raftWb.Set(y.KeyWithTs(k, RaftTS), []byte("entry"))
	}
	raftWb.WriteToRaft(raftDb)

	type tempHolder struct {
		raftLogGcTask     task
		expectedCollected uint64
		nonExistRange     [2]uint64
		existRange        [2]uint64
	}

	tbls := []tempHolder{
		{
			raftLogGcTask: task{
				data: &raftLogGCTask{
					raftEngine: raftDb,
					regionID:   regionId,
					startIdx:   uint64(0),
					endIdx:     uint64(10),
				},
				tp: taskTypeRaftLogGC,
			},
			expectedCollected: uint64(10),
			nonExistRange:     [...]uint64{0, 10},
			existRange:        [...]uint64{10, 100},
		},

		{
			raftLogGcTask: task{
				data: &raftLogGCTask{
					raftEngine: raftDb,
					regionID:   regionId,
					startIdx:   uint64(0),
					endIdx:     uint64(50),
				},
				tp: taskTypeRaftLogGC,
			},
			expectedCollected: uint64(40),
			nonExistRange:     [...]uint64{0, 50},
			existRange:        [...]uint64{50, 100},
		},

		{
			raftLogGcTask: task{
				data: &raftLogGCTask{
					raftEngine: raftDb,
					regionID:   regionId,
					startIdx:   uint64(50),
					endIdx:     uint64(50),
				},
				tp: taskTypeRaftLogGC,
			},
			expectedCollected: uint64(0),
			nonExistRange:     [...]uint64{0, 50},
			existRange:        [...]uint64{50, 100},
		},

		{
			raftLogGcTask: task{
				data: &raftLogGCTask{
					raftEngine: raftDb,
					regionID:   regionId,
					startIdx:   uint64(50),
					endIdx:     uint64(60),
				},
				tp: taskTypeRaftLogGC,
			},
			expectedCollected: uint64(10),
			nonExistRange:     [...]uint64{0, 60},
			existRange:        [...]uint64{60, 100},
		},
	}

	for _, h := range tbls {
		runner.handle(h.raftLogGcTask)
		res := <-taskResCh
		assert.Equal(t, h.expectedCollected, uint64(res))
		raftLogMustNotExist(t, raftDb, 1, h.nonExistRange[0], h.nonExistRange[1])
		raftLogMustExist(t, raftDb, 1, h.existRange[0], h.existRange[1])
	}
}

func raftLogMustNotExist(t *testing.T, db *badger.DB, regionId, startIdx, endIdx uint64) {
	for i := startIdx; i < endIdx; i++ {
		k := RaftLogKey(regionId, i)
		db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(k)
			assert.Equal(t, err, badger.ErrKeyNotFound)
			return nil
		})
	}
}

func raftLogMustExist(t *testing.T, db *badger.DB, regionId, startIdx, endIdx uint64) {
	for i := startIdx; i < endIdx; i++ {
		k := RaftLogKey(regionId, i)
		db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(k)
			assert.Nil(t, err)
			assert.NotNil(t, item)
			return nil
		})
	}
}
