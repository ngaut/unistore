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
	"io/ioutil"
	"os"
	"testing"

	"github.com/ngaut/unistore/tikv/mvcc"

	"github.com/ngaut/unistore/lockstore"

	"github.com/pingcap/badger"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/stretchr/testify/require"
)

func newTestEngines(t *testing.T) *Engines {
	engines := new(Engines)
	engines.kv = new(mvcc.DBBundle)
	var err error
	engines.kvPath, err = ioutil.TempDir("", "unistore_kv")
	require.Nil(t, err)
	kvOpts := badger.DefaultOptions
	kvOpts.Dir = engines.kvPath
	kvOpts.ValueDir = engines.kvPath
	kvOpts.ValueThreshold = 256
	engines.kv.DB, err = badger.Open(kvOpts)
	engines.kv.LockStore = lockstore.NewMemStore(16 * 1024)
	require.Nil(t, err)
	engines.raftPath, err = ioutil.TempDir("", "unistore_raft")
	require.Nil(t, err)
	raftOpts := badger.DefaultOptions
	raftOpts.Dir = engines.raftPath
	raftOpts.ValueDir = engines.raftPath
	raftOpts.ValueThreshold = 256
	engines.raft, err = badger.Open(raftOpts)
	require.Nil(t, err)
	return engines
}

func newTestPeerStorage(t *testing.T) *PeerStorage {
	engines := newTestEngines(t)
	err := BootstrapStore(engines, 1, 1)
	require.Nil(t, err)
	region, err := PrepareBootstrap(engines, 1, 1, 1)
	require.Nil(t, err)
	peerStore, err := NewPeerStorage(engines, region, nil, 1, "")
	require.Nil(t, err)
	return peerStore
}

func newTestPeerStorageFromEnts(t *testing.T, ents []eraftpb.Entry) *PeerStorage {
	peerStore := newTestPeerStorage(t)
	kvWB := new(WriteBatch)
	ctx := NewInvokeContext(peerStore)
	raftWB := new(WriteBatch)
	require.Nil(t, peerStore.Append(ctx, ents[1:], raftWB))
	ctx.ApplyState.truncatedIndex = ents[0].Index
	ctx.ApplyState.truncatedTerm = ents[0].Term
	ctx.ApplyState.appliedIndex = ents[len(ents)-1].Index
	ctx.saveApplyStateTo(kvWB)
	require.Nil(t, peerStore.Engines.WriteRaft(raftWB))
	peerStore.Engines.WriteKV(kvWB)
	peerStore.raftState = ctx.RaftState
	peerStore.applyState = ctx.ApplyState
	return peerStore
}

func cleanUpTestData(peerStore *PeerStorage) {
	os.RemoveAll(peerStore.Engines.kvPath)
	os.RemoveAll(peerStore.Engines.raftPath)
}

func cleanUpTestEngineData(engines *Engines) {
	os.RemoveAll(engines.kvPath)
	os.RemoveAll(engines.raftPath)
}

func newTestEntry(index, term uint64) eraftpb.Entry {
	return eraftpb.Entry{
		Index: index,
		Term:  term,
		Data:  []byte{0},
	}
}
