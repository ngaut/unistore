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

	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCmdResp(t *testing.T) {
	resp := new(raft_cmdpb.RaftCmdResponse)
	ensureRespHeader(resp)
	require.NotNil(t, resp.Header)

	term := uint64(1)
	BindRespTerm(resp, term)
	assert.Equal(t, resp.Header.CurrentTerm, term)

	regionID := uint64(2)
	notLeader := &ErrNotLeader{RegionID: regionID}
	BindRespError(resp, notLeader)
	require.NotNil(t, resp.Header.Error.NotLeader)
	assert.Equal(t, resp.Header.Error.NotLeader.RegionId, regionID)

	resp = ErrResp(notLeader)
	require.NotNil(t, resp.Header.Error.NotLeader)
	assert.Equal(t, resp.Header.Error.NotLeader.RegionId, regionID)

	resp = ErrRespWithTerm(notLeader, term)
	require.NotNil(t, resp.Header.Error.NotLeader)
	assert.Equal(t, resp.Header.CurrentTerm, term)
	assert.Equal(t, resp.Header.Error.NotLeader.RegionId, regionID)

	resp = ErrRespRegionNotFound(regionID)
	require.NotNil(t, resp.Header.Error.RegionNotFound)
	assert.Equal(t, resp.Header.Error.RegionNotFound.RegionId, regionID)
}
