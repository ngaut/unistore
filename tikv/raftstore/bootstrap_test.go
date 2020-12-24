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
	"os"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/stretchr/testify/require"
)

func TestBootstrapStore(t *testing.T) {
	engines := newTestEngines(t)
	defer func() {
		os.RemoveAll(engines.kvPath)
		os.RemoveAll(engines.raftPath)
	}()
	require.Nil(t, BootstrapStore(engines, 1, 1))
	require.NotNil(t, BootstrapStore(engines, 1, 1))
	_, err := PrepareBootstrap(engines, 1, 1, 1)
	require.Nil(t, err)
	region := new(metapb.Region)
	require.Nil(t, getKVMsg(engines.kv, prepareBootstrapKey, region))
	regionLocalState := new(rspb.RegionLocalState)
	require.Nil(t, getKVMsg(engines.kv, RegionStateKey(1), regionLocalState))
	raftApplyState := applyState{}
	val, err := getKVValue(engines.kv, ShardingApplyStateKey(rawInitialStartKey, 1))
	require.Nil(t, err)
	raftApplyState.Unmarshal(val)
	raftLocalState := raftState{}
	val, err = getValue(engines.raft, RaftStateKey(1))
	require.Nil(t, err)
	raftLocalState.Unmarshal(val)

	require.Nil(t, ClearPrepareBootstrapState(engines))
	require.Nil(t, ClearPrepareBootstrap(engines, 1))
	empty, err := isKVRaftCFRangeEmpty(engines.kv, rawInitialStartKey, rawInitialEndKey)
	require.Nil(t, err)
	require.True(t, empty)
}
