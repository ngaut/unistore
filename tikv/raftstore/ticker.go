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

type ticker struct {
	regionID  uint64
	tick      int64
	schedules []tickSchedule
}

type tickSchedule struct {
	runAt    int64
	interval int64
}

func newTicker(regionID uint64, cfg *Config) *ticker {
	baseInterval := cfg.RaftBaseTickInterval
	t := &ticker{
		regionID:  regionID,
		schedules: make([]tickSchedule, 6),
	}
	t.schedules[int(PeerTickRaft)].interval = 1
	t.schedules[int(PeerTickRaftLogGC)].interval = int64(cfg.RaftLogGCTickInterval / baseInterval)
	t.schedules[int(PeerTickSplitRegionCheck)].interval = int64(cfg.SplitRegionCheckTickInterval / baseInterval)
	t.schedules[int(PeerTickPdHeartbeat)].interval = int64(cfg.PdHeartbeatTickInterval / baseInterval)
	t.schedules[int(PeerTickCheckMerge)].interval = int64(cfg.MergeCheckTickInterval / baseInterval)
	t.schedules[int(PeerTickPeerStaleState)].interval = int64(cfg.PeerStaleStateCheckInterval / baseInterval)
	return t
}

func newStoreTicker(cfg *Config) *ticker {
	baseInterval := cfg.RaftBaseTickInterval
	t := &ticker{
		schedules: make([]tickSchedule, 4),
	}
	t.schedules[int(StoreTickCompactCheck)].interval = int64(cfg.RegionCompactCheckInterval / baseInterval)
	t.schedules[int(StoreTickPdStoreHeartbeat)].interval = int64(cfg.PdStoreHeartbeatTickInterval / baseInterval)
	t.schedules[int(StoreTickSnapGC)].interval = int64(cfg.SnapMgrGcTickInterval / baseInterval)
	t.schedules[int(StoreTickConsistencyCheck)].interval = int64(cfg.ConsistencyCheckInterval / baseInterval)
	return t
}

// tickClock should be called when peerMsgHandler received tick message.
func (t *ticker) tickClock() {
	t.tick++
}

// schedule arrange the next run for the PeerTick.
func (t *ticker) schedule(tp PeerTick) {
	sched := &t.schedules[int(tp)]
	if sched.interval <= 0 {
		sched.runAt = -1
		return
	}
	sched.runAt = t.tick + sched.interval
}

// isOnTick checks if the PeerTick should run.
func (t *ticker) isOnTick(tp PeerTick) bool {
	sched := &t.schedules[int(tp)]
	return sched.runAt == t.tick
}

func (t *ticker) isOnStoreTick(tp StoreTick) bool {
	sched := &t.schedules[int(tp)]
	return sched.runAt == t.tick
}

func (t *ticker) scheduleStore(tp StoreTick) {
	sched := &t.schedules[int(tp)]
	if sched.interval <= 0 {
		sched.runAt = -1
		return
	}
	sched.runAt = t.tick + sched.interval
}
