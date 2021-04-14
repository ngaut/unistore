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
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
)

func ensureRespHeader(resp *raft_cmdpb.RaftCmdResponse) {
	header := resp.GetHeader()
	if header == nil {
		resp.Header = &raft_cmdpb.RaftResponseHeader{}
	}
}

// BindRespTerm binds the RaftCmdResponse to the term.
func BindRespTerm(resp *raft_cmdpb.RaftCmdResponse, term uint64) {
	if term == 0 {
		return
	}
	ensureRespHeader(resp)
	resp.Header.CurrentTerm = term
}

// BindRespError binds the RaftCmdResponse to the error.
func BindRespError(resp *raft_cmdpb.RaftCmdResponse, err error) {
	ensureRespHeader(resp)
	resp.Header.Error = ErrToPbError(err)
}

// ErrResp returns a RaftCmdResponse which is bound to the error.
func ErrResp(err error) *raft_cmdpb.RaftCmdResponse {
	resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
	BindRespError(resp, err)
	return resp
}

// ErrRespWithTerm returns a RaftCmdResponse which is bound to the error and the term.
func ErrRespWithTerm(err error, term uint64) *raft_cmdpb.RaftCmdResponse {
	resp := ErrResp(err)
	BindRespTerm(resp, term)
	return resp
}

// ErrRespStaleCommand returns a RaftCmdResponse which is bound to the ErrStaleCommand and the term.
func ErrRespStaleCommand(term uint64) *raft_cmdpb.RaftCmdResponse {
	return ErrRespWithTerm(new(ErrStaleCommand), term)
}

// ErrRespRegionNotFound returns a RaftCmdResponse which is bound to the RegionNotFound error.
func ErrRespRegionNotFound(regionID uint64) *raft_cmdpb.RaftCmdResponse {
	return &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{
			Error: &errorpb.Error{
				Message: "region is not found",
				RegionNotFound: &errorpb.RegionNotFound{
					RegionId: regionID,
				},
			},
		},
	}
}

func newCmdRespForReq(req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	return &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{
			Uuid: req.Header.Uuid,
		},
	}
}
