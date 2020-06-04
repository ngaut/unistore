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

	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
)

const (
	InitEpochVer     uint64 = 1
	InitEpochConfVer uint64 = 1
	KvTS             uint64 = 1
	RaftTS           uint64 = 0
)

func isRangeEmpty(engine *badger.DB, startKey, endKey []byte) (bool, error) {
	var hasData bool
	err := engine.View(func(txn *badger.Txn) error {
		it := dbreader.NewIterator(txn, false, startKey, endKey)
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
	return !hasData, err
}

func BootstrapStore(engines *Engines, clussterID, storeID uint64) error {
	ident := new(rspb.StoreIdent)
	empty, err := isRangeEmpty(engines.kv.DB, MinKey, MaxDataKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("kv store is not empty and ahs alread had data.")
	}
	empty, err = isRangeEmpty(engines.raft, MinKey, MaxDataKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("raft store is not empty and has already had data.")
	}
	ident.ClusterId = clussterID
	ident.StoreId = storeID
	wb := new(WriteBatch)
	err = wb.SetMsg(y.KeyWithTs(storeIdentKey, KvTS), ident)
	if err != nil {
		return err
	}
	return wb.WriteToKV(engines.kv)
}

func PrepareBootstrap(engins *Engines, storeID, regionID, peerID uint64) (*metapb.Region, error) {
	region := &metapb.Region{
		Id:       regionID,
		StartKey: []byte{},
		EndKey:   []byte{},
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
	err := writePrepareBootstrap(engins, region)
	if err != nil {
		return nil, err
	}
	return region, nil
}

func writePrepareBootstrap(engines *Engines, region *metapb.Region) error {
	state := new(rspb.RegionLocalState)
	state.Region = region
	kvWB := new(WriteBatch)
	kvWB.SetMsg(y.KeyWithTs(prepareBootstrapKey, KvTS), state)
	kvWB.SetMsg(y.KeyWithTs(RegionStateKey(region.Id), KvTS), state)
	writeInitialApplyState(kvWB, region.Id)
	err := engines.WriteKV(kvWB)
	if err != nil {
		return err
	}
	err = engines.SyncKVWAL()
	if err != nil {
		return err
	}
	raftWB := new(WriteBatch)
	writeInitialRaftState(raftWB, region.Id)
	err = engines.WriteRaft(raftWB)
	if err != nil {
		return err
	}
	return engines.SyncRaftWAL()
}

func writeInitialApplyState(kvWB *WriteBatch, regionID uint64) {
	applyState := applyState{
		appliedIndex:   RaftInitLogIndex,
		truncatedIndex: RaftInitLogIndex,
		truncatedTerm:  RaftInitLogTerm,
	}
	kvWB.Set(y.KeyWithTs(ApplyStateKey(regionID), KvTS), applyState.Marshal())
}

func writeInitialRaftState(raftWB *WriteBatch, regionID uint64) {
	raftState := raftState{
		lastIndex: RaftInitLogIndex,
		term:      RaftInitLogTerm,
		commit:    RaftInitLogIndex,
	}
	raftWB.Set(y.KeyWithTs(RaftStateKey(regionID), RaftTS), raftState.Marshal())
}

func ClearPrepareBootstrap(engines *Engines, regionID uint64) error {
	err := engines.raft.Update(func(txn *badger.Txn) error {
		return txn.Delete(RaftStateKey(regionID))
	})
	if err != nil {
		return errors.WithStack(err)
	}
	wb := new(WriteBatch)
	wb.Delete(y.KeyWithTs(prepareBootstrapKey, KvTS))
	// should clear raft initial state too.
	wb.Delete(y.KeyWithTs(RegionStateKey(regionID), KvTS))
	wb.Delete(y.KeyWithTs(ApplyStateKey(regionID), KvTS))
	err = engines.WriteKV(wb)
	if err != nil {
		return err
	}
	return engines.SyncKVWAL()
}

func ClearPrepareBootstrapState(engines *Engines) error {
	wb := new(WriteBatch)
	wb.Delete(y.KeyWithTs(prepareBootstrapKey, KvTS))
	err := wb.WriteToKV(engines.kv)
	return errors.WithStack(err)
}
