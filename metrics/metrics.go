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

package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	namespace = "tikv"
	raft      = "raft"
)

var (
	RaftWriterWait = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: raft,
			Name:      "writer_wait",
			Buckets:   prometheus.ExponentialBuckets(0.001, 1.5, 20),
		})
	WriteWaiteStepOne = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: raft,
			Name:      "writer_wait_step_1",
			Buckets:   prometheus.ExponentialBuckets(0.001, 1.5, 20),
		})
	WriteWaiteStepTwo = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: raft,
			Name:      "writer_wait_step_2",
			Buckets:   prometheus.ExponentialBuckets(0.001, 1.5, 20),
		})
	WriteWaiteStepThree = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: raft,
			Name:      "writer_wait_step_3",
			Buckets:   prometheus.ExponentialBuckets(0.001, 1.5, 20),
		})
	WriteWaiteStepFour = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: raft,
			Name:      "writer_wait_step_4",
			Buckets:   prometheus.ExponentialBuckets(0.001, 1.5, 20),
		})

	RaftDBUpdate = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: raft,
			Name:      "raft_db_update",
			Buckets:   prometheus.ExponentialBuckets(0.001, 1.5, 20),
		})
	KVDBUpdate = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: raft,
			Name:      "kv_db_update",
			Buckets:   prometheus.ExponentialBuckets(0.001, 1.5, 20),
		})
	LockUpdate = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: raft,
			Name:      "lock_update",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
		})
	LatchWait = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: raft,
			Name:      "latch_wait",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
		})
	RaftBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: raft,
			Name:      "batch_size",
			Buckets:   prometheus.ExponentialBuckets(1, 1.5, 20),
		})
	StoreSizeBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "store",
			Name:      "size_bytes",
			Help:      "Size of storage.",
		}, []string{"type"})
	RaftstoreRegionCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "raftstore",
			Name:      "region_count",
			Help:      "Number of regions collected in region_collector",
		}, []string{"type"})
	EngineSizeBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "engine",
			Name:      "size_bytes",
			Help:      "Sizes of each column families",
		}, []string{"db", "type"})
	EngineFlowBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "engine",
			Name:      "flow_bytes",
			Help:      "Bytes and keys of read/written",
		}, []string{"db", "type"})
)

func init() {
	prometheus.MustRegister(RaftWriterWait)
	prometheus.MustRegister(WriteWaiteStepOne)
	prometheus.MustRegister(WriteWaiteStepTwo)
	prometheus.MustRegister(WriteWaiteStepThree)
	prometheus.MustRegister(WriteWaiteStepFour)
	prometheus.MustRegister(RaftDBUpdate)
	prometheus.MustRegister(KVDBUpdate)
	prometheus.MustRegister(LockUpdate)
	prometheus.MustRegister(RaftBatchSize)
	prometheus.MustRegister(LatchWait)
	prometheus.MustRegister(StoreSizeBytes)
	prometheus.MustRegister(RaftstoreRegionCount)
	prometheus.MustRegister(EngineSizeBytes)
	prometheus.MustRegister(EngineFlowBytes)
	http.Handle("/metrics", promhttp.Handler())
}
