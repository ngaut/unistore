package raftstore

import (
	stdatomic "sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/uber-go/atomic"
)

type LeaseChecker interface {
	CheckLease(req *raft_cmdpb.RaftCmdRequest, snapTime time.Time) (bool, error)
}

type leaseChecker struct {
	invalid          atomic.Bool
	peerID           atomic.Uint64
	term             atomic.Uint64
	appliedIndexTerm atomic.Uint64
	leaderLease      unsafe.Pointer // *RemoteLease
	region           unsafe.Pointer // *metapb.Region
}

func (c *leaseChecker) CheckLease(req *raft_cmdpb.RaftCmdRequest, snapTime time.Time) (bool, error) {
	if c.invalid.Load() {
		return false, nil
	}

	if err := checkPeerID(req, c.peerID.Load()); err != nil {
		return false, err
	}

	if err := checkTerm(req, c.term.Load()); err != nil {
		return false, err
	}

	region := (*metapb.Region)(stdatomic.LoadPointer(&c.region))
	if err := checkRegionEpoch(req, region, false); err != nil {
		return false, err
	}

	inspector := leaseCheckerInspector{
		checker:  c,
		snapTime: &snapTime,
	}
	policy, err := inspector.inspect(req)
	if err != nil {
		return false, err
	}
	return policy == RequestPolicy_ReadLocal, nil
}

type leaseCheckerInspector struct {
	checker  *leaseChecker
	snapTime *time.Time
}

func (i *leaseCheckerInspector) hasAppliedToCurrentTerm() bool {
	return i.checker.appliedIndexTerm.Load() == i.checker.term.Load()
}

func (i *leaseCheckerInspector) inspectLease() LeaseState {
	lease := (*RemoteLease)(stdatomic.LoadPointer(&i.checker.leaderLease))
	return lease.Inspect(i.snapTime)
}

func (i *leaseCheckerInspector) inspect(req *raft_cmdpb.RaftCmdRequest) (RequestPolicy, error) {
	return Inspect(i, req)
}
