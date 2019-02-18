package worker

import "github.com/pingcap/kvproto/pkg/raft_cmdpb"

func GetChangePeerCmd(msg *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.ChangePeerRequest {
	adminReq := msg.GetAdminRequest()
	if adminReq == nil || adminReq.ChangePeer == nil{
		return nil
	}
	return adminReq.ChangePeer
}