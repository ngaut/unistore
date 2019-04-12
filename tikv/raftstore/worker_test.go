package raftstore

import (
	"github.com/coocood/badger"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGcRaftLog(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpEngineData(engines)
	raftDb := engines.raft
	taskResCh := make(chan raftLogGcTaskRes, 1)
	runner := raftLogGCRunner{taskResCh: taskResCh}

	//  generate raft logs
	regionId := uint64(1)
	raftWb := new(WriteBatch)
	for i := uint64(0); i < 100; i++ {
		k := RaftLogKey(regionId, i)
		raftWb.Set(k, []byte("entry"))
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
		runner.run(h.raftLogGcTask)
		res := <-taskResCh
		assert.Equal(t, uint64(res), h.expectedCollected)
		raftLogMustNotExist(t, raftDb, 1, h.nonExistRange[0], h.nonExistRange[1])
		raftLogMustExist(t, raftDb, 1, h.existRange[0], h.existRange[1])
	}
}

func raftLogMustNotExist(t *testing.T, db *badger.DB, regionId, startIdx, endIdx uint64) {
	for i := startIdx; i < endIdx; i++ {
		k := RaftLogKey(regionId, i)
		db.View(func (txn *badger.Txn) error {
			_, err := txn.Get(k)
			assert.Equal(t, err, badger.ErrKeyNotFound)
			return nil
		})
	}
}

func raftLogMustExist(t *testing.T, db *badger.DB, regionId, startIdx, endIdx uint64) {
	for i := startIdx; i < endIdx; i++ {
		k := RaftLogKey(regionId, i)
		db.View(func (txn *badger.Txn) error {
			item, err := txn.Get(k)
			assert.Nil(t, err)
			assert.NotNil(t, item)
			return nil
		})
	}
}
