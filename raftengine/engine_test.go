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
	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestEngine(t *testing.T) {
	dir, err := ioutil.TempDir("", "raft_engine")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	walSize := int64(128 * 1024)
	e, err := Open(dir, walSize)
	require.Nil(t, err)
	for index := uint64(1); index <= 1050; index++ {
		wb := NewWriteBatch()
		for regionID := uint64(1); regionID <= 10; regionID++ {
			if regionID == 1 && (index > 100 && index < 900) {
				continue
			}
			wb.AppendRaftLog(regionID, index, makeLogData(regionID, index, 128))
			key, val := makeStateKV(regionID, index)
			wb.SetState(regionID, key, val)
			if index%100 == 0 && regionID != 1 {
				wb.TruncateRaftLog(regionID, index-100)
			}
		}
		err = e.Write(wb)
		time.Sleep(time.Millisecond * 3)
		require.Nil(t, err)
	}
	require.Nil(t, e.Close())
	log.S().Infof("start reloading")
	for i := 0; i < 2; i++ {
		e2, err := Open(dir, walSize)
		require.Nil(t, err, errors.Errors(err))
		e2.states.Ascend(func(i btree.Item) bool {
			require.True(t, e.states.Has(i))
			return true
		})
		e.states.Ascend(func(i btree.Item) bool {
			require.True(t, e2.states.Has(i))
			return true
		})

		require.Equal(t, 10, len(e2.entriesMap))
		for regionID, entries := range e2.entriesMap {
			old := e.entriesMap[regionID]
			require.Equal(t, len(old.raftLogs), len(entries.raftLogs), "%d %d %d %d", old.startIndex, old.endIndex, entries.startIndex, entries.endIndex)
			require.Equal(t, int(old.startIndex), int(entries.startIndex))
			require.Equal(t, old.endIndex, entries.endIndex)
			for i := old.startIndex; i < old.endIndex; i++ {
				require.Equal(t, old.get(i), entries.get(i))
			}
		}
		e2.Close()
	}
}

func makeLogData(regionID, index uint64, size int) []byte {
	data := make([]byte, size)
	binary.LittleEndian.PutUint64(data, regionID)
	binary.LittleEndian.PutUint64(data[8:], index)
	return data
}

func makeStateKV(regionID, index uint64) (key, val []byte) {
	key = make([]byte, 8)
	val = make([]byte, 8)
	binary.LittleEndian.PutUint64(key, regionID)
	binary.LittleEndian.PutUint64(val, index)
	return
}
