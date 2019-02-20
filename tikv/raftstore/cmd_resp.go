package raftstore

import (
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/errors"
)

func RaftstoreErrToPbError(e error) *errorpb.Error {
	ret := new(errorpb.Error)
	if notLeader, ok := errors.Cause(e).(*ErrNotLeader); ok {
		ret.NotLeader = &errorpb.NotLeader{ RegionId: notLeader.RegionId, Leader: notLeader.Leader }
		return ret
	}
	if regionNotFound, ok := errors.Cause(e).(*ErrRegionNotFound); ok {
		ret.RegionNotFound = &errorpb.RegionNotFound{ RegionId: regionNotFound.RegionId }
		return ret
	}
	if keyNotInRegion, ok := errors.Cause(e).(*ErrKeyNotInRegion); ok {
		ret.KeyNotInRegion = &errorpb.KeyNotInRegion{ Key: keyNotInRegion.Key, RegionId: keyNotInRegion.Region.Id,
			StartKey: keyNotInRegion.Region.StartKey, EndKey: keyNotInRegion.Region.EndKey }
		return ret
	}
	if epochNotMatch, ok := errors.Cause(e).(*ErrEpochNotMatch); ok {
		ret.EpochNotMatch = &errorpb.EpochNotMatch{ CurrentRegions: epochNotMatch.Regions }
		return ret
	}
	if serverIsBusy, ok := errors.Cause(e).(*ErrServerIsBusy); ok {
		ret.ServerIsBusy = &errorpb.ServerIsBusy{ Reason: serverIsBusy.Reason, BackoffMs: serverIsBusy.BackoffMs }
		return ret
	}
	if _, ok := errors.Cause(e).(*ErrStaleCommand); ok {
		ret.StaleCommand = &errorpb.StaleCommand{}
		return ret
	}
	if storeNotMatch, ok := errors.Cause(e).(*ErrStoreNotMatch); ok {
		ret.StoreNotMatch = &errorpb.StoreNotMatch{ RequestStoreId: storeNotMatch.RequestStoreId, ActualStoreId: storeNotMatch.ActualStoreId }
		return ret
	}
	if raftEntryTooLarge, ok := errors.Cause(e).(*ErrRaftEntryTooLarge); ok {
		ret.RaftEntryTooLarge = &errorpb.RaftEntryTooLarge{ RegionId: raftEntryTooLarge.RegionId, EntrySize: raftEntryTooLarge.EntrySize }
		return ret
	}

	ret.Message = e.Error()
	return ret
}

func respEnsureHeader(resp *raft_cmdpb.RaftCmdResponse) {
	header := resp.GetHeader()
	if header == nil {
		resp.Header = &raft_cmdpb.RaftResponseHeader{}
	}
}

func BindTerm(resp *raft_cmdpb.RaftCmdResponse, term uint64) {
	if term == 0 {
		return
	}
	respEnsureHeader(resp)
	resp.Header.CurrentTerm = term
}

func BindError(resp *raft_cmdpb.RaftCmdResponse, err error) {
	respEnsureHeader(resp)
	resp.Header.Error = RaftstoreErrToPbError(err)
}

func NewError(err error) *raft_cmdpb.RaftCmdResponse {
	resp := &raft_cmdpb.RaftCmdResponse {Header: &raft_cmdpb.RaftResponseHeader{}}
	BindError(resp, err)
	return resp
}

func ErrResp(err error, term uint64) *raft_cmdpb.RaftCmdResponse {
	resp := NewError(err)
	BindTerm(resp, term)
	return resp
}
