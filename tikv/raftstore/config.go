package raftstore

import "time"

type Config struct {
	SyncLog bool
	Prevote bool
	RaftdbPath string
	Capacity uint64
	RaftBaseTickInterval time.Duration
	RaftHeartbeatTicks uint
	RaftElectionTimeoutTicks uint
	RaftMinElectionTimeoutTicks uint
	RaftMaxElectionTimeoutTicks uint
	RaftMaxSizePerMsg uint64
	RaftMaxInflightMsgs uint
	RaftEntryMaxSize uint64
	RaftLogGCTickInterval uint64
	RaftLogGcThreshold uint64
	RaftLogGcCountLimit uint64
	RaftLogGcSizeLimit uint64
	RaftEntryCacheLifeTime time.Duration
	RaftRejectTransferLeaderDuration time.Duration
	SplitRegionCheckTickInterval time.Duration
	RegionSplitCheckDiff uint64
	RegionCompactCheckInterval time.Duration
	CleanStalePeerDelay time.Duration
	RegionCompactCheckStep uint64
	RegionCompactMinTombstones uint64
	RegionCompactTombstonesPencent uint64
	PdHeartbeatTickInterval time.Duration
	PdStoreHeartbeatTickInterval time.Duration
	SnapMgrGcTickInterval time.Duration
	SnapGcTimeout time.Duration
	LockCfCompactInterval time.Duration
	LockCfCompactBytesThreshold uint64
	NotifyCapacity uint64
	MessagesPerTick uint64
	MaxPeerDownDuration time.Duration
	MaxLeaderMissingDuration time.Duration
	AbnormalLeaderMissingDuration time.Duration
	PeerStaleStateCheckInterval time.Duration
	LeaderTransferMaxLogLag uint64
	SnapApplyBatchSize uint64
	ReportRegionFlowInterval time.Duration
	RaftStoreMaxLeaderLease time.Duration
	RightDeriveWhenSplit bool
	AllowRemoveLeader bool
	MergeMaxLogGap uint64
	MergeCheckTickInterval time.Duration
	CleanupImportSstInterval time.Duration
	LocalReadBatchSize uint64
	ApplyMaxBatchSize uint64
	ApplyPoolSize uint64
	StoreMaxBatchSize uint64
	StorePoolSize uint64
	FuturePoolSize uint64
	RegionMaxSize uint64
	RegionSplitSize uint64
}