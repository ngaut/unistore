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
	"bytes"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"math"
)

const (
	InitEpochVer     uint64 = 1
	InitEpochConfVer uint64 = 1
	RaftTS           uint64 = 0
)

func isRaftRangeEmpty(engine *badger.DB, startKey, endKey []byte) (bool, error) {
	var hasData bool
	err := engine.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			StartKey: y.Key{UserKey: startKey, Version: math.MaxUint64},
			EndKey:   y.Key{UserKey: endKey, Version: 0},
		})
		defer it.Close()
		it.Seek(startKey)
		if it.Valid() {
			item := it.Item()
			if bytes.Compare(item.Key(), endKey) < 0 {
				hasData = true
			}
		}
		return nil
	})
	if err != nil {
		return false, errors.WithStack(err)
	}
	return !hasData, nil
}

func BootstrapStore(engines *Engines, clussterID, storeID uint64) error {
	ident := new(rspb.StoreIdent)
	if engines.kv.Size() != 0 {
		return errors.New("kv store is not empty and ahs alread had data.")
	}
	empty, err := isRaftRangeEmpty(engines.raft, MinKey, MaxDataKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("raft store is not empty and has already had data.")
	}
	ident.ClusterId = clussterID
	ident.StoreId = storeID
	wb := new(RaftWriteBatch)
	val, err := ident.Marshal()
	if err != nil {
		return err
	}
	wb.Set(y.KeyWithTs(storeIdentKey, 0), val)
	return wb.WriteToRaft(engines.raft)
}

var (
	rawInitialStartKey = []byte{2}
	rawInitialEndKey   = []byte{255, 255, 255, 255, 255, 255, 255, 255}
)

func PrepareBootstrap(engines *Engines, storeID, regionID, peerID uint64) (*metapb.Region, error) {
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			Version: InitEpochVer,
			ConfVer: InitEpochConfVer,
		},
		Peers: []*metapb.Peer{
			{
				Id:      peerID,
				StoreId: storeID,
			},
		},
	}
	err := writePrepareBootstrap(engines, region)
	if err != nil {
		return nil, err
	}
	return region, nil
}

func writePrepareBootstrap(engines *Engines, region *metapb.Region) error {
	state := new(rspb.RegionLocalState)
	state.Region = region
	raftWB := new(RaftWriteBatch)
	val, _ := state.Marshal()
	raftWB.Set(y.KeyWithTs(prepareBootstrapKey, RaftTS), val)
	raftWB.Set(y.KeyWithTs(RegionStateKey(region), RaftTS), val)
	writeInitialRaftState(raftWB, region)
	err := engines.WriteRaft(raftWB)
	if err != nil {
		return err
	}
	ingestTree := &badger.IngestTree{
		ChangeSet: &protos.ShardChangeSet{
			ShardID:  region.Id,
			ShardVer: region.RegionEpoch.Version,
			Snapshot: &protos.ShardSnapshot{
				Start: nil,
				End:   []byte{255, 255, 255, 255, 255, 255, 255, 255},
				Properties: &protos.ShardProperties{
					ShardID: region.Id,
					Keys:    []string{applyStateKey},
					Values:  [][]byte{newInitialApplyState().Marshal()},
				},
			},
		},
	}
	return engines.kv.Ingest(ingestTree)
}

func writeInitialRaftState(raftWB *RaftWriteBatch, region *metapb.Region) {
	raftState := raftState{
		lastIndex: RaftInitLogIndex,
		term:      RaftInitLogTerm,
		commit:    RaftInitLogIndex,
	}
	raftWB.Set(y.KeyWithTs(RaftStateKey(region), RaftTS), raftState.Marshal())
}

func ClearPrepareBootstrap(engines *Engines, region *metapb.Region) error {
	err := engines.raft.Update(func(txn *badger.Txn) error {
		return txn.Delete(RaftStateKey(region))
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return engines.kv.RemoveShard(region.Id, true)
}

func ClearPrepareBootstrapState(engines *Engines) error {
	wb := new(RaftWriteBatch)
	wb.Delete(y.KeyWithTs(prepareBootstrapKey, RaftTS))
	err := wb.WriteToRaft(engines.raft)
	return errors.WithStack(err)
}
