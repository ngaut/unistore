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
	"runtime"
	"time"

	"github.com/pingcap/log"
)

type Config struct {
	Server         Server         `toml:"server"` // Unistore server options
	Engine         Engine         `toml:"engine"` // Engine options.
	RaftEngine     RaftEngine     `toml:"raft-engine"`
	RaftStore      RaftStore      `toml:"raftstore"`       // RaftStore configs
	PessimisticTxn PessimisticTxn `toml:"pessimistic-txn"` // Pessimistic txn related
}

type Server struct {
	PDAddr      string            `toml:"pd-addr"`
	StoreAddr   string            `toml:"store-addr"`
	StatusAddr  string            `toml:"status-addr"`
	LogLevel    string            `toml:"log-level"`
	RegionSize  int64             `toml:"region-size"` // Average region size.
	MaxProcs    int               `toml:"max-procs"`   // Max CPU cores to use, set 0 to use all CPU cores in the machine.
	GCPercent   int               `toml:"gc-percent"`  // GC percent
	LogfilePath string            `toml:"log-file"`    // Log file path for unistore server
	Labels      map[string]string `toml:"labels"`
}

type RaftStore struct {
	PdHeartbeatTickInterval  string `toml:"pd-heartbeat-tick-interval"`  // pd-heartbeat-tick-interval in seconds
	RaftStoreMaxLeaderLease  string `toml:"raft-store-max-leader-lease"` // raft-store-max-leader-lease in milliseconds
	RaftBaseTickInterval     string `toml:"raft-base-tick-interval"`     // raft-base-tick-interval in milliseconds
	RaftHeartbeatTicks       int    `toml:"raft-heartbeat-ticks"`        // raft-heartbeat-ticks times
	RaftElectionTimeoutTicks int    `toml:"raft-election-timeout-ticks"` // raft-election-timeout-ticks times
	ApplyWorkerCount         int    `toml:"apply-worker-count"`
	GRPCRaftConnNum          int    `toml:"grpc-raft-conn-num"`
}

type Engine struct {
	Path                   string    `toml:"path"`           // Directory to store the data in. Should exist and be writable.
	MaxTableSize           int64     `toml:"max-table-size"` // Each table file is at most this size.
	BaseSize               int64     `toml:"base-size"`
	NumL0Tables            int       `toml:"num-L0-tables"`       // Maximum number of Level 0 tables before we start compacting.
	NumL0TablesStall       int       `toml:"num-L0-tables-stall"` // Maximum number of Level 0 tables before stalling.
	NumCompactors          int       `toml:"num-compactors"`
	BlockCacheSize         int64     `toml:"block-cache-size"`
	MaxMemTableSizeFactor  int       `toml:"max-mem-table-size-factor"` // Each mem table is at most this size.
	RemoteCompactionAddr   string    `toml:"remote-compaction-addr"`
	InstanceID             uint32    `toml:"instance-id"`
	S3                     S3Options `toml:"s3"`
	RecoveryConcurrency    int       `toml:"recovery-concurrency"`
	PreparationConcurrency int       `toml:"preparation-concurrency"`
}

type RaftEngine struct {
	Path    string `toml:"path"`
	WALSize int64  `toml:"wal-size"`
}

type S3Options struct {
	Endpoint           string `toml:"endpoint"`
	KeyID              string `toml:"key-id"`
	SecretKey          string `toml:"secret-key"`
	Bucket             string `toml:"bucket"`
	Region             string `toml:"region"`
	ExpirationDuration string `toml:"expiration-duration"`
	SimulateLatency    string `toml:"simulate-latency"`
	Concurrency        int    `toml:"concurrency"`
}

type PessimisticTxn struct {
	// The default and maximum delay in milliseconds before responding to TiDB when pessimistic
	// transactions encounter locks
	WaitForLockTimeout int64 `toml:"wait-for-lock-timeout"`

	// The duration between waking up lock waiter, in milliseconds
	WakeUpDelayDuration int64 `toml:"wake-up-delay-duration"`
}

const MB = 1024 * 1024

var DefaultConf = Config{
	Server: Server{
		PDAddr:      "127.0.0.1:2379",
		StoreAddr:   "127.0.0.1:9191",
		StatusAddr:  "127.0.0.1:9291",
		RegionSize:  64 * MB,
		LogLevel:    "info",
		MaxProcs:    0,
		GCPercent:   20,
		LogfilePath: "",
	},
	RaftStore: RaftStore{
		PdHeartbeatTickInterval:  "20s",
		RaftStoreMaxLeaderLease:  "9s",
		RaftBaseTickInterval:     "1s",
		RaftHeartbeatTicks:       2,
		RaftElectionTimeoutTicks: 10,
		ApplyWorkerCount:         4,
		GRPCRaftConnNum:          2,
	},
	Engine: Engine{
		Path:                   "/tmp/badger",
		MaxMemTableSizeFactor:  128,
		MaxTableSize:           8 * MB,
		NumL0Tables:            4,
		NumL0TablesStall:       8,
		NumCompactors:          3,
		BaseSize:               64 * MB,
		BlockCacheSize:         0, // 0 means disable block cache, use mmap to access sst.
		RecoveryConcurrency:    runtime.NumCPU(),
		PreparationConcurrency: runtime.NumCPU(),
	},
	RaftEngine: RaftEngine{
		Path:    "/tmp/badger",
		WALSize: 1024 * MB,
	},
	PessimisticTxn: PessimisticTxn{
		WaitForLockTimeout:  1000, // 1000ms same with tikv default value
		WakeUpDelayDuration: 100,  // 100ms same with tikv default value
	},
}

// parseDuration parses duration argument string.
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
