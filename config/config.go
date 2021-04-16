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

package config

import (
	"time"

	"github.com/pingcap/badger/options"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/mockstore/unistore/config"
)

// Config contains configuration options.
type Config struct {
	config.Config
	RaftStore RaftStore `toml:"raftstore"` // RaftStore configs
}

// RaftStore is the config for raft store.
type RaftStore struct {
	PdHeartbeatTickInterval  string `toml:"pd-heartbeat-tick-interval"`  // pd-heartbeat-tick-interval in seconds
	RaftStoreMaxLeaderLease  string `toml:"raft-store-max-leader-lease"` // raft-store-max-leader-lease in milliseconds
	RaftBaseTickInterval     string `toml:"raft-base-tick-interval"`     // raft-base-tick-interval in milliseconds
	RaftHeartbeatTicks       int    `toml:"raft-heartbeat-ticks"`        // raft-heartbeat-ticks times
	RaftElectionTimeoutTicks int    `toml:"raft-election-timeout-ticks"` // raft-election-timeout-ticks times
	CustomRaftLog            bool   `toml:"custom-raft-log"`
}

// ParseCompression parses the string s and returns a compression type.
func ParseCompression(s string) options.CompressionType {
	switch s {
	case "snappy":
		return options.Snappy
	case "zstd":
		return options.ZSTD
	default:
		return options.None
	}
}

// MB represents the MB size.
const MB = 1024 * 1024

// DefaultConf returns the default configuration.
var DefaultConf = Config{
	Config: config.DefaultConf,
	RaftStore: RaftStore{
		PdHeartbeatTickInterval:  "20s",
		RaftStoreMaxLeaderLease:  "9s",
		RaftBaseTickInterval:     "1s",
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
		CustomRaftLog:            true,
	},
}

// ParseDuration parses duration argument string.
func ParseDuration(durationStr string) time.Duration {
	dur, err := time.ParseDuration(durationStr)
	if err != nil {
		dur, err = time.ParseDuration(durationStr + "s")
	}
	if err != nil || dur < 0 {
		log.S().Fatalf("invalid duration=%v", durationStr)
	}
	return dur
}
