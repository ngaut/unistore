package raftstore

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
)

type readDelegate struct {
	region           *metapb.Region
	peerID           uint64
	term             uint64
	appliedIndexTerm uint64
	leaderLease      *RemoteLease
	lastValidTs      time.Time
}

func newReadDelegate(peer *Peer) *readDelegate {
	return &readDelegate{
		region:           peer.Region(),
		peerID:           peer.PeerId(),
		term:             peer.Term(),
		appliedIndexTerm: peer.Store().appliedIndexTerm,
		leaderLease:      nil,
	}
}

func (rd *readDelegate) updateRegion(region *metapb.Region) {
	rd.region = region
}

func (rd *readDelegate) updateTerm(term uint64) {
	rd.term = term
}

func (rd *readDelegate) updateAppliedIndexTerm(term uint64) {
	rd.appliedIndexTerm = term
}

func (rd *readDelegate) updateLeaderLease(lease *RemoteLease) {
	rd.leaderLease = lease
}

func (rd *readDelegate) canExecuteCmdLocally(req *raft_cmdpb.RaftCmdRequest, snapTime time.Time) (bool, error) {
	if err := checkPeerID(req, rd.peerID); err != nil {
		return false, err
	}

	if err := checkTerm(req, rd.term); err != nil {
		return false, err
	}

	if err := checkRegionEpoch(req, rd.region, false); err != nil {
		return false, err
	}

	if req.GetAdminRequest() != nil {
		return false, nil
	}

	var hasRead, hasWrite bool
	for _, r := range req.Requests {
		switch r.CmdType {
		case raft_cmdpb.CmdType_Get, raft_cmdpb.CmdType_Snap:
			hasRead = true
		case raft_cmdpb.CmdType_Delete, raft_cmdpb.CmdType_Put, raft_cmdpb.CmdType_DeleteRange, raft_cmdpb.CmdType_IngestSST:
			hasWrite = true
		case raft_cmdpb.CmdType_Prewrite, raft_cmdpb.CmdType_Invalid:
			return false, errors.New("invalid cmd type, message maybe corrupted")
		}

		if hasRead && hasWrite {
			return false, errors.New("read and write can't be mixed in one batch.")
		}
	}
	if hasWrite || req.GetHeader().GetReadQuorum() {
		return false, nil
	}

	// If applied index's term is differ from current raft's term, leader transfer
	// must happened, if read locally, we may read old value.
	if rd.appliedIndexTerm != rd.term {
		return false, nil
	}

	if rd.leaderLease == nil || rd.leaderLease.Term() != rd.term {
		return false, nil
	}

	if rd.lastValidTs.Equal(snapTime) || rd.leaderLease.Inspect(&snapTime) == LeaseState_Valid {
		rd.lastValidTs = snapTime
		return true, nil
	}

	return false, nil
}
