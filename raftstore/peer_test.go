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

	"github.com/ngaut/unistore/tikv/raftstore/raftlog"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/stretchr/testify/assert"
)

func TestGetSyncLogFromRequest(t *testing.T) {
	allTypes := map[raft_cmdpb.AdminCmdType]bool{
		raft_cmdpb.AdminCmdType_InvalidAdmin:   false,
		raft_cmdpb.AdminCmdType_ChangePeer:     true,
		raft_cmdpb.AdminCmdType_Split:          true,
		raft_cmdpb.AdminCmdType_CompactLog:     false,
		raft_cmdpb.AdminCmdType_TransferLeader: false,
		raft_cmdpb.AdminCmdType_ComputeHash:    false,
		raft_cmdpb.AdminCmdType_VerifyHash:     false,
		raft_cmdpb.AdminCmdType_PrepareMerge:   true,
		raft_cmdpb.AdminCmdType_CommitMerge:    true,
		raft_cmdpb.AdminCmdType_RollbackMerge:  true,
		raft_cmdpb.AdminCmdType_BatchSplit:     true,
	}

	for tp, sync := range allTypes {
		req := new(raft_cmdpb.RaftCmdRequest)
		req.AdminRequest = new(raft_cmdpb.AdminRequest)
		req.AdminRequest.CmdType = tp

		assert.Equal(t, getSyncLogFromRequest(req), sync)
	}
}

func TestIsUrgentRequest(t *testing.T) {
	allTypes := map[raft_cmdpb.AdminCmdType]bool{
		raft_cmdpb.AdminCmdType_InvalidAdmin:   false,
		raft_cmdpb.AdminCmdType_ChangePeer:     true,
		raft_cmdpb.AdminCmdType_Split:          true,
		raft_cmdpb.AdminCmdType_CompactLog:     false,
		raft_cmdpb.AdminCmdType_TransferLeader: false,
		raft_cmdpb.AdminCmdType_ComputeHash:    true,
		raft_cmdpb.AdminCmdType_VerifyHash:     true,
		raft_cmdpb.AdminCmdType_PrepareMerge:   true,
		raft_cmdpb.AdminCmdType_CommitMerge:    true,
		raft_cmdpb.AdminCmdType_RollbackMerge:  true,
		raft_cmdpb.AdminCmdType_BatchSplit:     true,
	}
	for tp, isUrgent := range allTypes {
		req := new(raft_cmdpb.RaftCmdRequest)
		req.AdminRequest = new(raft_cmdpb.AdminRequest)
		req.AdminRequest.CmdType = tp

		assert.Equal(t, IsUrgentRequest(raftlog.NewRequest(req)), isUrgent)
	}
	assert.Equal(t, IsUrgentRequest(raftlog.NewRequest(new(raft_cmdpb.RaftCmdRequest))), false)
}

func TestEntryCtx(t *testing.T) {
	tbl := [][]ProposalContext{
		{ProposalContextSplit},
		{ProposalContextSyncLog},
		{ProposalContextPrepareMerge},
		{ProposalContextSplit, ProposalContextSyncLog},
		{ProposalContextPrepareMerge, ProposalContextSyncLog},
	}
	for _, flags := range tbl {
		var ctx ProposalContext
		for _, f := range flags {
			ctx.insert(f)
		}

		ser := ctx.ToBytes()
		de := NewProposalContextFromBytes(ser)

		for _, f := range flags {
			assert.True(t, de.contains(f))
		}
	}
}

type DummyInspector struct {
	AppliedToIndexTerm bool
	LeaseState         LeaseState
}

func (i *DummyInspector) hasAppliedToCurrentTerm() bool {
	return i.AppliedToIndexTerm
}

func (i *DummyInspector) inspectLease() LeaseState {
	return i.LeaseState
}

func (i *DummyInspector) inspect(req *raft_cmdpb.RaftCmdRequest) (RequestPolicy, error) {
	return Inspect(i, req)
}

type ReqPolicyPair struct {
	Req    *raft_cmdpb.RaftCmdRequest
	Policy RequestPolicy
}

type OpPolicyPair struct {
	Tp     raft_cmdpb.CmdType
	Policy RequestPolicy
}

func TestRequestInspector(t *testing.T) {
	var tbl []ReqPolicyPair

	// Ok(_)
	req := new(raft_cmdpb.RaftCmdRequest)
	req.AdminRequest = new(raft_cmdpb.AdminRequest)
	tbl = append(tbl, ReqPolicyPair{Req: req, Policy: RequestPolicyProposeNormal})

	req = new(raft_cmdpb.RaftCmdRequest)
	admReq := new(raft_cmdpb.AdminRequest)
	admReq.ChangePeer = new(raft_cmdpb.ChangePeerRequest)
	req.AdminRequest = admReq
	tbl = append(tbl, ReqPolicyPair{Req: req, Policy: RequestPolicyProposeConfChange})

	req = new(raft_cmdpb.RaftCmdRequest)
	admReq = new(raft_cmdpb.AdminRequest)
	admReq.TransferLeader = new(raft_cmdpb.TransferLeaderRequest)
	req.AdminRequest = admReq
	tbl = append(tbl, ReqPolicyPair{Req: req, Policy: RequestPolicyProposeTransferLeader})

	Ops := []OpPolicyPair{
		{Tp: raft_cmdpb.CmdType_Get, Policy: RequestPolicyReadLocal},
		{Tp: raft_cmdpb.CmdType_Snap, Policy: RequestPolicyReadLocal},
		{Tp: raft_cmdpb.CmdType_Put, Policy: RequestPolicyProposeNormal},
		{Tp: raft_cmdpb.CmdType_Delete, Policy: RequestPolicyProposeNormal},
		{Tp: raft_cmdpb.CmdType_DeleteRange, Policy: RequestPolicyProposeNormal},
		{Tp: raft_cmdpb.CmdType_IngestSST, Policy: RequestPolicyProposeNormal},
	}
	for _, opPolicy := range Ops {
		request := new(raft_cmdpb.Request)
		request.CmdType = opPolicy.Tp
		req = new(raft_cmdpb.RaftCmdRequest)
		req.Requests = []*raft_cmdpb.Request{request}
		tbl = append(tbl, ReqPolicyPair{Req: req, Policy: opPolicy.Policy})
	}

	for _, appliedToIndexTerm := range []bool{true, false} {
		for _, leaseState := range []LeaseState{LeaseStateExpired, LeaseStateSuspect, LeaseStateValid} {
			for _, reqPolicy := range tbl {
				policy := reqPolicy.Policy
				inspector := &DummyInspector{
					AppliedToIndexTerm: appliedToIndexTerm,
					LeaseState:         leaseState,
				}
				// Leader can not read local as long as
				// it has not applied to its term or it does has a valid lease.
				if policy == RequestPolicyReadLocal && (!appliedToIndexTerm || LeaseStateValid != inspector.LeaseState) {
					policy = RequestPolicyReadIndex
				}
				inspectPolicy, err := inspector.inspect(reqPolicy.Req)
				assert.Nil(t, err)
				assert.Equal(t, inspectPolicy, policy)
			}
		}
	}

	// Read quorum
	request := new(raft_cmdpb.Request)
	request.CmdType = raft_cmdpb.CmdType_Snap
	req = new(raft_cmdpb.RaftCmdRequest)
	req.Requests = []*raft_cmdpb.Request{request}
	req.Header = new(raft_cmdpb.RaftRequestHeader)
	req.Header.ReadQuorum = true
	inspector := DummyInspector{
		AppliedToIndexTerm: true,
		LeaseState:         LeaseStateValid,
	}
	inspectPolicy, err := inspector.inspect(req)
	assert.Nil(t, err)
	assert.Equal(t, inspectPolicy, RequestPolicyReadIndex)

	// Err(_)
	var errTbl []*raft_cmdpb.RaftCmdRequest
	for _, op := range []raft_cmdpb.CmdType{raft_cmdpb.CmdType_Prewrite, raft_cmdpb.CmdType_Invalid} {
		request := new(raft_cmdpb.Request)
		request.CmdType = op
		req = new(raft_cmdpb.RaftCmdRequest)
		req.Requests = []*raft_cmdpb.Request{request}
		errTbl = append(errTbl, req)
	}
	snap := new(raft_cmdpb.Request)
	snap.CmdType = raft_cmdpb.CmdType_Snap
	put := new(raft_cmdpb.Request)
	put.CmdType = raft_cmdpb.CmdType_Put
	req = new(raft_cmdpb.RaftCmdRequest)
	req.Requests = []*raft_cmdpb.Request{snap, put}

	for _, req := range errTbl {
		inspector := DummyInspector{
			AppliedToIndexTerm: true,
			LeaseState:         LeaseStateValid,
		}
		_, err := inspector.inspect(req)
		assert.NotNil(t, err)
	}
}
