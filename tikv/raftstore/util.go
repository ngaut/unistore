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
	"bytes"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ngaut/unistore/tikv/raftstore/raftlog"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/mvcc"
)

// util
const (
	RaftInvalidIndex uint64 = 0
	InvalidID        uint64 = 0
)

// `is_initial_msg` checks whether the `msg` can be used to initialize a new peer or not.
// There could be two cases:
// 1. Target peer already exists but has not established communication with leader yet
// 2. Target peer is added newly due to member change or region split, but it's not
//    created yet
// For both cases the region start key and end key are attached in RequestVote and
// Heartbeat message for the store of that peer to check whether to create a new peer
// when receiving these messages, or just to wait for a pending region split to perform
// later.
func isInitialMsg(msg *eraftpb.Message) bool {
	return msg.MsgType == eraftpb.MessageType_MsgRequestVote ||
		msg.MsgType == eraftpb.MessageType_MsgRequestPreVote ||
		// the peer has not been known to this leader, it may exist or not.
		(msg.MsgType == eraftpb.MessageType_MsgHeartbeat && msg.Commit == RaftInvalidIndex)
}

// LeaseState represents the lease state.
type LeaseState int

// LeaseState
const (
	// The lease is suspicious, may be invalid.
	LeaseStateSuspect LeaseState = 1 + iota
	// The lease is valid.
	LeaseStateValid
	// The lease is expired.
	LeaseStateExpired
)

// Lease records an expired time, for examining the current moment is in lease or not.
// It's dedicated to the Raft leader lease mechanism, contains either state of
//   1. Suspect Timestamp
//      A suspicious leader lease timestamp, which marks the leader may still hold or lose
//      its lease until the clock time goes over this timestamp.
//   2. Valid Timestamp
//      A valid leader lease timestamp, which marks the leader holds the lease for now.
//      The lease is valid until the clock time goes over this timestamp.
//
// ```text
// Time
// |---------------------------------->
//         ^               ^
//        Now           Suspect TS
// State:  |    Suspect    |   Suspect
//
// |---------------------------------->
//         ^               ^
//        Now           Valid TS
// State:  |     Valid     |   Expired
// ```
//
// Note:
//   - Valid timestamp would increase when raft log entries are applied in current term.
//   - Suspect timestamp would be set after the message `MsgTimeoutNow` is sent by current peer.
//     The message `MsgTimeoutNow` starts a leader transfer procedure. During this procedure,
//     current peer as an old leader may still hold its lease or lose it.
//     It's possible there is a new leader elected and current peer as an old leader
//     doesn't step down due to network partition from the new leader. In that case,
//     current peer lose its leader lease.
//     Within this suspect leader lease expire time, read requests could not be performed
//     locally.
//   - The valid leader lease should be `lease = max_lease - (commit_ts - send_ts)`
//     And the expired timestamp for that leader lease is `commit_ts + lease`,
//     which is `send_ts + max_lease` in short.
type Lease struct {
	// If boundSuspect is not nil, then boundValid must be nil, if boundValid is not nil, then
	// boundSuspect must be nil
	boundSuspect *time.Time
	boundValid   *time.Time
	maxLease     time.Duration

	maxDrift   time.Duration
	lastUpdate time.Time
	remote     *RemoteLease

	// Todo: use monotonic_raw instead of time.Now() to fix time jump back issue.
}

// NewLease creates a new Lease.
func NewLease(maxLease time.Duration) *Lease {
	return &Lease{
		maxLease:   maxLease,
		maxDrift:   maxLease / 3,
		lastUpdate: time.Time{},
	}
}

// The valid leader lease should be `lease = max_lease - (commit_ts - send_ts)`
// And the expired timestamp for that leader lease is `commit_ts + lease`,
// which is `send_ts + max_lease` in short.
func (l *Lease) nextExpiredTime(sendTs time.Time) time.Time {
	return sendTs.Add(l.maxLease)
}

// Renew the lease to the bound.
func (l *Lease) Renew(sendTs time.Time) {
	bound := l.nextExpiredTime(sendTs)
	if l.boundSuspect != nil {
		// Longer than suspect ts
		if l.boundSuspect.Before(bound) {
			l.boundSuspect = nil
			l.boundValid = &bound
		}
	} else if l.boundValid != nil {
		// Longer than valid ts
		if l.boundValid.Before(bound) {
			l.boundValid = &bound
		}
	} else {
		// Or an empty lease
		l.boundValid = &bound
	}

	// Renew remote if it's valid.
	if l.boundValid != nil {
		if l.boundValid.Sub(l.lastUpdate) > l.maxDrift {
			l.lastUpdate = *l.boundValid
			if l.remote != nil {
				l.remote.Renew(*l.boundValid)
			}
		}
	}
}

// Suspect the lease to the bound.
func (l *Lease) Suspect(sendTs time.Time) {
	l.ExpireRemoteLease()
	bound := l.nextExpiredTime(sendTs)
	l.boundValid = nil
	l.boundSuspect = &bound
}

// Inspect the lease state for the ts or now.
func (l *Lease) Inspect(ts *time.Time) LeaseState {
	if l.boundSuspect != nil {
		return LeaseStateSuspect
	}
	if l.boundValid != nil {
		if ts == nil {
			t := time.Now()
			ts = &t
		}
		if ts.Before(*l.boundValid) {
			return LeaseStateValid
		}
		return LeaseStateExpired
	}
	return LeaseStateExpired
}

// Expire sets the lease state to expired.
func (l *Lease) Expire() {
	l.ExpireRemoteLease()
	l.boundValid = nil
	l.boundSuspect = nil
}

// ExpireRemoteLease sets the remote lease state to expired.
func (l *Lease) ExpireRemoteLease() {
	// Expire remote lease if there is any.
	if l.remote != nil {
		l.remote.Expire()
		l.remote = nil
	}
}

// MaybeNewRemoteLease returns a new `RemoteLease` if there is none.
func (l *Lease) MaybeNewRemoteLease(term uint64) *RemoteLease {
	if l.remote != nil {
		if l.remote.term == term {
			// At most one connected RemoteLease in the same term.
			return nil
		}
		// Term has changed. It is unreachable in the current implementation,
		// because we expire remote lease when leaders step down.
		panic("Must expire the old remote lease first!")
	}
	expiredTime := uint64(0)
	if l.boundValid != nil {
		expiredTime = TimeToU64(*l.boundValid)
	}
	remote := &RemoteLease{
		expiredTime: &expiredTime,
		term:        term,
	}
	// Clone the remote.
	remoteClone := &RemoteLease{
		expiredTime: &expiredTime,
		term:        term,
	}
	l.remote = remote
	return remoteClone
}

// RemoteLease represents a remote lease, it can only be derived by `Lease`. It will be sent
// to the local read thread, so name it remote. If Lease expires, the remote must
// expire too.
type RemoteLease struct {
	expiredTime *uint64
	term        uint64
}

// Inspect returns the lease state with the given time.
func (r *RemoteLease) Inspect(ts *time.Time) LeaseState {
	expiredTime := atomic.LoadUint64(r.expiredTime)
	if ts == nil {
		t := time.Now()
		ts = &t
	}
	if ts.Before(U64ToTime(expiredTime)) {
		return LeaseStateValid
	}
	return LeaseStateExpired
}

// Renew renews the lease to the bound.
func (r *RemoteLease) Renew(bound time.Time) {
	atomic.StoreUint64(r.expiredTime, TimeToU64(bound))
}

// Expire sets the remote lease state to expired.
func (r *RemoteLease) Expire() {
	atomic.StoreUint64(r.expiredTime, 0)
}

// Term returns the term of the RemoteLease.
func (r *RemoteLease) Term() uint64 {
	return r.term
}

// util
const (
	NsecPerMsec uint64 = 1000000
	SecShift    uint64 = 10
	MsecMask    uint64 = (1 << SecShift) - 1
)

// TimeToU64 converts time t to a uint64 type.
func TimeToU64(t time.Time) uint64 {
	sec := uint64(t.Unix())
	msec := uint64(t.Nanosecond()) / NsecPerMsec
	sec <<= SecShift
	return sec | msec
}

// U64ToTime converts uint64 u to a time value.
func U64ToTime(u uint64) time.Time {
	sec := u >> SecShift
	nsec := (u & MsecMask) * NsecPerMsec
	return time.Unix(int64(sec), int64(nsec))
}

// CheckKeyInRegion checks if key in region range [`start_key`, `end_key`).
func CheckKeyInRegion(key []byte, region *metapb.Region) error {
	if bytes.Compare(key, region.StartKey) >= 0 && (len(region.EndKey) == 0 || bytes.Compare(key, region.EndKey) < 0) {
		return nil
	}
	return &ErrKeyNotInRegion{Key: key, Region: region}
}

// CheckKeyInRegionExclusive checks if key in region range (`start_key`, `end_key`).
func CheckKeyInRegionExclusive(key []byte, region *metapb.Region) error {
	if bytes.Compare(region.StartKey, key) < 0 && (len(region.EndKey) == 0 || bytes.Compare(key, region.EndKey) < 0) {
		return nil
	}
	return &ErrKeyNotInRegion{Key: key, Region: region}
}

// CheckKeyInRegionInclusive check if key in region range [`start_key`, `end_key`].
func CheckKeyInRegionInclusive(key []byte, region *metapb.Region) error {
	if bytes.Compare(key, region.StartKey) >= 0 && (len(region.EndKey) == 0 || bytes.Compare(key, region.EndKey) <= 0) {
		return nil
	}
	return &ErrKeyNotInRegion{Key: key, Region: region}
}

// IsEpochStale checks whether epoch is staler than check epoch.
func IsEpochStale(epoch *metapb.RegionEpoch, checkEpoch *metapb.RegionEpoch) bool {
	return epoch.Version < checkEpoch.Version || epoch.ConfVer < checkEpoch.ConfVer
}

// CheckRegionEpoch checks the region epoch.
func CheckRegionEpoch(req *raft_cmdpb.RaftCmdRequest, region *metapb.Region, includeRegion bool) error {
	checkVer, checkConfVer := false, false
	if req.AdminRequest == nil {
		checkVer = true
	} else {
		switch req.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog, raft_cmdpb.AdminCmdType_InvalidAdmin,
			raft_cmdpb.AdminCmdType_ComputeHash, raft_cmdpb.AdminCmdType_VerifyHash:
		case raft_cmdpb.AdminCmdType_ChangePeer:
			checkConfVer = true
		case raft_cmdpb.AdminCmdType_Split, raft_cmdpb.AdminCmdType_BatchSplit,
			raft_cmdpb.AdminCmdType_PrepareMerge, raft_cmdpb.AdminCmdType_CommitMerge,
			raft_cmdpb.AdminCmdType_RollbackMerge, raft_cmdpb.AdminCmdType_TransferLeader:
			checkVer = true
			checkConfVer = true
		}
	}

	if !checkVer && !checkConfVer {
		return nil
	}

	if req.Header == nil {
		return fmt.Errorf("missing header")
	}

	if req.Header.RegionEpoch == nil {
		return fmt.Errorf("missing epoch")
	}

	fromEpoch := req.Header.RegionEpoch
	currentEpoch := region.RegionEpoch

	// We must check epochs strictly to avoid key not in region error.
	//
	// A 3 nodes TiKV cluster with merge enabled, after commit merge, TiKV A
	// tells TiDB with a epoch not match error contains the latest target Region
	// info, TiDB updates its region cache and sends requests to TiKV B,
	// and TiKV B has not applied commit merge yet, since the region epoch in
	// request is higher than TiKV B, the request must be denied due to epoch
	// not match, so it does not read on a stale snapshot, thus avoid the
	// KeyNotInRegion error.
	if (checkConfVer && fromEpoch.ConfVer != currentEpoch.ConfVer) ||
		(checkVer && fromEpoch.Version != currentEpoch.Version) {
		log.S().Debugf("epoch not match, region id %v, from epoch %v, current epoch %v",
			region.Id, fromEpoch, currentEpoch)

		regions := []*metapb.Region{}
		if includeRegion {
			regions = []*metapb.Region{region}
		}
		return &ErrEpochNotMatch{Message: fmt.Sprintf("current epoch of region %v is %v, but you sent %v",
			region.Id, currentEpoch, fromEpoch), Regions: regions}
	}

	return nil
}

func findPeer(region *metapb.Region, storeID uint64) *metapb.Peer {
	for _, peer := range region.Peers {
		if peer.StoreId == storeID {
			return peer
		}
	}
	return nil
}

func removePeer(region *metapb.Region, storeID uint64) *metapb.Peer {
	for i, peer := range region.Peers {
		if peer.StoreId == storeID {
			region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
			return peer
		}
	}
	return nil
}

func isVoteMessage(msg *eraftpb.Message) bool {
	tp := msg.GetMsgType()
	return tp == eraftpb.MessageType_MsgRequestVote || tp == eraftpb.MessageType_MsgRequestPreVote
}

// `is_first_vote_msg` checks `msg` is the first vote (or prevote) message or not. It's used for
// when the message is received but there is no such region in `Store::region_peers` and the
// region overlaps with others. In this case we should put `msg` into `pending_votes` instead of
// create the peer.
func isFirstVoteMessage(msg *eraftpb.Message) bool {
	return isVoteMessage(msg) && msg.Term == RaftInitLogTerm+1
}

func regionIDToBytes(id uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, id)
	return b
}

func regionIDFromBytes(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

func checkRegionEpoch(rlog raftlog.RaftLog, region *metapb.Region, includeRegion bool) error {
	var checkVer, checkConfVer bool
	req := rlog.GetRaftCmdRequest()
	if req.GetAdminRequest() == nil {
		// for get/set/delete, we don't care conf_version.
		checkVer = true
	} else {
		switch req.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog, raft_cmdpb.AdminCmdType_InvalidAdmin,
			raft_cmdpb.AdminCmdType_ComputeHash, raft_cmdpb.AdminCmdType_VerifyHash:
		case raft_cmdpb.AdminCmdType_ChangePeer:
			checkConfVer = true
		case raft_cmdpb.AdminCmdType_Split, raft_cmdpb.AdminCmdType_BatchSplit,
			raft_cmdpb.AdminCmdType_PrepareMerge, raft_cmdpb.AdminCmdType_CommitMerge,
			raft_cmdpb.AdminCmdType_RollbackMerge, raft_cmdpb.AdminCmdType_TransferLeader:
			checkVer = true
			checkConfVer = true
		}
	}
	if !checkVer && !checkConfVer {
		return nil
	}
	currentEpoch := region.RegionEpoch

	// We must check epochs strictly to avoid key not in region error.
	//
	// A 3 nodes TiKV cluster with merge enabled, after commit merge, TiKV A
	// tells TiDB with a epoch not match error contains the latest target Region
	// info, TiDB updates its region cache and sends requests to TiKV B,
	// and TiKV B has not applied commit merge yet, since the region epoch in
	// request is higher than TiKV B, the request must be denied due to epoch
	// not match, so it does not read on a stale snapshot, thus avoid the
	// KeyNotInRegion error.
	if (checkConfVer && rlog.Epoch().ConfVer() != currentEpoch.ConfVer) ||
		(checkVer && rlog.Epoch().Ver() != currentEpoch.Version) {
		err := &ErrEpochNotMatch{}
		if includeRegion {
			err.Regions = []*metapb.Region{region}
		}
		err.Message = fmt.Sprintf("current epoch of region %d is %s, but you sent %s",
			region.Id, currentEpoch, rlog.Epoch())
		return err
	}
	return nil
}

func checkStoreID(rlog raftlog.RaftLog, storeID uint64) error {
	if rlog.StoreID() == storeID {
		return nil
	}
	return errors.Errorf("store not match %d %d", rlog.StoreID(), storeID)
}

func checkTerm(rlog raftlog.RaftLog, term uint64) error {
	if rlog.Term() == 0 || term <= rlog.Term()+1 {
		return nil
	}
	// If header's term is 2 verions behind current term,
	// leadership may have been changed away.
	return &ErrStaleCommand{}
}

func checkPeerID(rlog raftlog.RaftLog, peerID uint64) error {
	if rlog.PeerID() == peerID {
		return nil
	}
	return errors.Errorf("mismatch peer id %d != %d", rlog.PeerID(), peerID)
}

// CloneMsg clones the Message.
func CloneMsg(origin, cloned proto.Message) error {
	data, err := proto.Marshal(origin)
	if err != nil {
		return err
	}
	return proto.Unmarshal(data, cloned)
}

func deleteAllFilesInRange(db *mvcc.DBBundle, startKey, endKey []byte) error {
	// todo, needs badger to export api to support delete files.
	return nil
}
