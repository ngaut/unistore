package raftstore

import (
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/stretchr/testify/assert"
	"testing"
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

		assert.Equal(t, IsUrgentRequest(req), isUrgent)
	}
	assert.Equal(t, IsUrgentRequest(new(raft_cmdpb.RaftCmdRequest)), false)
}

func TestEntryCtx(t *testing.T) {
	tbl := [][]ProposalContext{
		[]ProposalContext{ProposalContext_Split},
		[]ProposalContext{ProposalContext_SyncLog},
		[]ProposalContext{ProposalContext_PrepareMerge},
		[]ProposalContext{ProposalContext_Split, ProposalContext_SyncLog},
		[]ProposalContext{ProposalContext_PrepareMerge, ProposalContext_SyncLog},
	}
	for _, flags := range tbl {
		var ctx ProposalContext
		for _, f := range flags {
			ctx.insert(f)
		}

		ser := ctx.ToVec()
		de := NewProposalContextFromBytes(ser)

		for _, f := range flags {
			assert.True(t, de.contains(f))
		}
	}
}
