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

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRaftstoreErrToPbError(t *testing.T) {
	regionId := uint64(1)
	notLeader := &ErrNotLeader{RegionId: regionId, Leader: nil}
	pbErr := RaftstoreErrToPbError(notLeader)
	require.NotNil(t, pbErr.NotLeader)
	assert.Equal(t, pbErr.NotLeader.RegionId, regionId)

	regionNotFound := &ErrRegionNotFound{RegionId: regionId}
	pbErr = RaftstoreErrToPbError(regionNotFound)
	require.NotNil(t, pbErr.RegionNotFound)
	assert.Equal(t, pbErr.RegionNotFound.RegionId, regionId)

	region := &metapb.Region{Id: regionId, StartKey: []byte{0}, EndKey: []byte{1}}

	keyNotInRegion := &ErrKeyNotInRegion{Key: []byte{2}, Region: region}
	pbErr = RaftstoreErrToPbError(keyNotInRegion)
	require.NotNil(t, pbErr.KeyNotInRegion)
	assert.Equal(t, pbErr.KeyNotInRegion.StartKey, []byte{0})
	assert.Equal(t, pbErr.KeyNotInRegion.EndKey, []byte{1})
	assert.Equal(t, pbErr.KeyNotInRegion.Key, []byte{2})

	epochNotMatch := &ErrEpochNotMatch{Regions: []*metapb.Region{region}}
	pbErr = RaftstoreErrToPbError(epochNotMatch)
	require.NotNil(t, pbErr.EpochNotMatch)
	assert.Equal(t, pbErr.EpochNotMatch.CurrentRegions, []*metapb.Region{region})

	backOffMs := uint64(10)
	serverIsBusy := &ErrServerIsBusy{Reason: "tikv is busy", BackoffMs: backOffMs}
	pbErr = RaftstoreErrToPbError(serverIsBusy)
	require.NotNil(t, pbErr.ServerIsBusy)
	assert.Equal(t, pbErr.ServerIsBusy.Reason, "tikv is busy")
	assert.Equal(t, pbErr.ServerIsBusy.BackoffMs, backOffMs)

	staleCommand := &ErrStaleCommand{}
	pbErr = RaftstoreErrToPbError(staleCommand)
	require.NotNil(t, pbErr.StaleCommand)

	requestStoreId, actualStoreId := uint64(1), uint64(2)
	storeNotMatch := &ErrStoreNotMatch{RequestStoreId: requestStoreId, ActualStoreId: actualStoreId}
	pbErr = RaftstoreErrToPbError(storeNotMatch)
	require.NotNil(t, pbErr.StoreNotMatch)
	assert.Equal(t, pbErr.StoreNotMatch.RequestStoreId, requestStoreId)
	assert.Equal(t, pbErr.StoreNotMatch.ActualStoreId, actualStoreId)

	entrySize := uint64(10000000)
	raftEntryTooLarge := &ErrRaftEntryTooLarge{RegionId: regionId, EntrySize: entrySize}
	pbErr = RaftstoreErrToPbError(raftEntryTooLarge)
	require.NotNil(t, pbErr.RaftEntryTooLarge)
	assert.Equal(t, pbErr.RaftEntryTooLarge.RegionId, regionId)
	assert.Equal(t, pbErr.RaftEntryTooLarge.EntrySize, entrySize)
}
