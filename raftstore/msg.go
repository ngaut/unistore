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
	"sync"
	"time"

	"github.com/ngaut/unistore/raftstore/raftlog"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/zhangjinpeng1987/raft"
)

// MsgType represents a msg type.
type MsgType int64

// Msg
const (
	MsgTypeNull                   MsgType = 0
	MsgTypeRaftMessage            MsgType = 1
	MsgTypeRaftCmd                MsgType = 2
	MsgTypeSplitRegion            MsgType = 3
	MsgTypeComputeResult          MsgType = 4
	MsgTypeRegionApproximateSize  MsgType = 5
	MsgTypeRegionApproximateKeys  MsgType = 6
	MsgTypeCompactionDeclineBytes MsgType = 7
	MsgTypeHalfSplitRegion        MsgType = 8
	MsgTypeMergeResult            MsgType = 9
	MsgTypeGcSnap                 MsgType = 10
	MsgTypeClearRegionSize        MsgType = 11
	MsgTypeTick                   MsgType = 12
	MsgTypeSignificantMsg         MsgType = 13
	MsgTypeStart                  MsgType = 14
	MsgTypeApplyRes               MsgType = 15
	MsgTypeNoop                   MsgType = 16

	MsgTypeStoreRaftMessage   MsgType = 101
	MsgTypeStoreSnapshotStats MsgType = 102
	// Clear region size and keys for all regions in the range, so we can force them to re-calculate
	// their size later.
	MsgTypeStoreClearRegionSizeInRange MsgType = 104
	MsgTypeStoreCompactedEvent         MsgType = 105
	MsgTypeStoreTick                   MsgType = 106
	MsgTypeStoreStart                  MsgType = 107

	MsgTypeFsmNormal  MsgType = 201
	MsgTypeFsmControl MsgType = 202

	MsgTypeApply             MsgType = 301
	MsgTypeApplyRegistration MsgType = 302
	MsgTypeApplyProposal     MsgType = 303
	MsgTypeApplyCatchUpLogs  MsgType = 304
	MsgTypeApplyLogsUpToDate MsgType = 305
	MsgTypeApplyDestroy      MsgType = 306
	MsgTypeApplySnapshot     MsgType = 307
)

// Msg represents a message.
type Msg struct {
	Type     MsgType
	RegionID uint64
	Data     interface{}
}

// NewPeerMsg creates a peer msg.
func NewPeerMsg(tp MsgType, regionID uint64, data interface{}) Msg {
	return Msg{Type: tp, RegionID: regionID, Data: data}
}

// NewMsg creates a msg.
func NewMsg(tp MsgType, data interface{}) Msg {
	return Msg{Type: tp, Data: data}
}

// Callback represents a callback.
type Callback struct {
	resp           *raft_cmdpb.RaftCmdResponse
	wg             sync.WaitGroup
	raftBeginTime  time.Time
	raftDoneTime   time.Time
	applyBeginTime time.Time
	applyDoneTime  time.Time
}

// Done sets the RaftCmdResponse and calls Done() on the WaitGroup.
func (cb *Callback) Done(resp *raft_cmdpb.RaftCmdResponse) {
	if cb != nil {
		cb.resp = resp
		cb.wg.Done()
	}
}

// NewCallback creates a new Callback.
func NewCallback() *Callback {
	cb := &Callback{}
	cb.wg.Add(1)
	return cb
}

// PeerTick represents a peer tick.
type PeerTick int

// PeerTick
const (
	PeerTickRaft             PeerTick = 0
	PeerTickRaftLogGC        PeerTick = 1
	PeerTickSplitRegionCheck PeerTick = 2
	PeerTickPdHeartbeat      PeerTick = 3
	PeerTickCheckMerge       PeerTick = 4
	PeerTickPeerStaleState   PeerTick = 5
)

// StoreTick represents a store tick.
type StoreTick int

// StoreTick
const (
	StoreTickCompactCheck     StoreTick = 0
	StoreTickPdStoreHeartbeat StoreTick = 1
	StoreTickSnapGC           StoreTick = 2
	StoreTickConsistencyCheck StoreTick = 3
)

// MsgSignificantType represents a significant type of msg.
type MsgSignificantType int

// MsgSignificantType
const (
	MsgSignificantTypeStatus      MsgSignificantType = 1
	MsgSignificantTypeUnreachable MsgSignificantType = 2
)

// MsgSignificant represents a significant msg.
type MsgSignificant struct {
	Type           MsgSignificantType
	ToPeerID       uint64
	SnapshotStatus raft.SnapshotStatus
}

// MsgRaftCmd defines a message of raft command.
type MsgRaftCmd struct {
	SendTime time.Time
	Request  raftlog.RaftLog
	Callback *Callback
}

// MsgSplitRegion defines a message which is used to split region.
type MsgSplitRegion struct {
	RegionEpoch *metapb.RegionEpoch
	// It's an encoded key.
	// TODO: support meta key.
	SplitKeys [][]byte
	Callback  *Callback
}

// MsgComputeHashResult defines a message which is used to compute hash result.
type MsgComputeHashResult struct {
	Index uint64
	Hash  []byte
}

// MsgHalfSplitRegion defines a message which is used to split region in half.
type MsgHalfSplitRegion struct {
	RegionEpoch *metapb.RegionEpoch
}

// MsgMergeResult defines a message which is used to merge result.
type MsgMergeResult struct {
	TargetPeer *metapb.Peer
	Stale      bool
}

// SnapKeyWithSending represents a snapshot key with sending.
type SnapKeyWithSending struct {
	SnapKey   SnapKey
	IsSending bool
}

// MsgGCSnap defines a message which is used to collect snapshot.
type MsgGCSnap struct {
	Snaps []SnapKeyWithSending
}

// MsgStoreClearRegionSizeInRange defines a message which is used to clear region size in range.
type MsgStoreClearRegionSizeInRange struct {
	StartKey []byte
	EndKey   []byte
}

func newApplyMsg(apply *apply) Msg {
	return Msg{Type: MsgTypeApply, Data: apply}
}
