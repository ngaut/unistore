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
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/log"
	"math"
)

const (
	InitEpochVer     uint64 = 1
	InitEpochConfVer uint64 = 1
	KvTS             uint64 = 0
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

func isKVRaftCFRangeEmpty(engine *badger.ShardingDB, startKey, endKey []byte) (bool, error) {
	var hasData bool
	dbSnap := engine.NewSnapshot(startKey, endKey)
	it := dbSnap.NewIterator(mvcc.RaftCF, false, false)
	it.Seek(startKey)
	if it.Valid() {
		item := it.Item()
		if bytes.Compare(item.Key(), endKey) < 0 {
			hasData = true
		}
	}
	return !hasData, nil
}

func BootstrapStore(engines *Engines, clussterID, storeID uint64) error {
	ident := new(rspb.StoreIdent)
	empty, err := isKVRaftCFRangeEmpty(engines.kv, MinKey, MaxDataKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("kv store is not empty and ahs alread had data.")
	}
	empty, err = isRaftRangeEmpty(engines.raft, MinKey, MaxDataKey)
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("raft store is not empty and has already had data.")
	}
	ident.ClusterId = clussterID
	ident.StoreId = storeID
	wb := new(WriteBatch)
	val, err := ident.Marshal()
	if err != nil {
		return err
	}
	wb.SetRaftCF(y.KeyWithTs(storeIdentKey, 0), val)
	return wb.WriteToKV(engines.kv)
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
	kvWB := new(WriteBatch)
	val, _ := state.Marshal()
	kvWB.SetRaftCF(y.KeyWithTs(prepareBootstrapKey, KvTS), val)
	kvWB.SetRaftCF(y.KeyWithTs(RegionStateKey(region.Id), KvTS), val)
	writeInitialApplyState(kvWB, region.Id)
	err := engines.WriteKV(kvWB)
	if err != nil {
		return err
	}
	log.S().Infof("write initial region local state")
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
	err = engines.metaManager.prepareBootstrap(region)
	if err != nil {
		return err
	}
	return engines.SyncRaftWAL()
}

func writeInitialApplyState(kvWB *WriteBatch, regionID uint64) {
	applyState := applyState{
		appliedIndex:     RaftInitLogIndex,
		appliedIndexTerm: RaftInitLogTerm,
		truncatedIndex:   RaftInitLogIndex,
		truncatedTerm:    RaftInitLogTerm,
	}
	kvWB.SetRaftCF(y.KeyWithTs(ShardingApplyStateKey(rawInitialStartKey, regionID), KvTS), applyState.Marshal())
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
	wb.SetRaftCF(y.KeyWithTs(prepareBootstrapKey, KvTS), nil)
	// should clear raft initial state too.
	wb.SetRaftCF(y.KeyWithTs(RegionStateKey(regionID), KvTS), nil)
	wb.SetRaftCF(y.KeyWithTs(ShardingApplyStateKey(rawInitialStartKey, regionID), KvTS), nil)
	err = engines.WriteKV(wb)
	if err != nil {
		return err
	}
	engines.metaManager.clearPrepareBootstrap(regionID)
	return engines.SyncKVWAL()
}

func ClearPrepareBootstrapState(engines *Engines) error {
	wb := new(WriteBatch)
	wb.SetRaftCF(y.KeyWithTs(prepareBootstrapKey, KvTS), nil)
	err := wb.WriteToKV(engines.kv)
	return errors.WithStack(err)
}
