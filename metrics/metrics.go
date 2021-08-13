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
	ThreadCPUSecondsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "thread",
			Name:      "cpu_seconds_total",
			Help:      "Total user and system CPU time spent in seconds by threads.",
		}, []string{"name", "pid"})
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
	GrpcMsgDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "grpc",
			Name:      "msg_duration_seconds",
			Help:      "Bucketed histogram of grpc server messages",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
		}, []string{"type"})
	GrpcMsgFailTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "grpc",
			Name:      "msg_fail_total",
			Help:      "Total number of handle grpc message failure",
		}, []string{"type"})
	ServerGrpcReqBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "server",
			Name:      "grpc_req_batch_size",
			Help:      "grpc batch size of gRPC requests",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
		})
	ServerGrpcRespBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "server",
			Name:      "grpc_resp_batch_size",
			Help:      "grpc batch size of gRPC responses",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
		})
	ServerRaftMessageBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "server",
			Name:      "raft_message_batch_size",
			Help:      "Raft messages batch size",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
		})
	RegionWrittenKeys = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "region",
			Name:      "written_keys",
			Help:      "Histogram of keys written for regions",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
		})
	RegionWrittenBytes = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "region",
			Name:      "written_bytes",
			Help:      "Histogram of bytes written for regions",
			Buckets:   prometheus.ExponentialBuckets(256, 2, 20),
		})
	RegionReadKeys = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "region",
			Name:      "read_keys",
			Help:      "Histogram of keys read for regions",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
		})
	RegionReadBytes = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "region",
			Name:      "read_bytes",
			Help:      "Histogram of bytes read for regions",
			Buckets:   prometheus.ExponentialBuckets(256, 2, 20),
		})
	WorkerHandledTaskTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "worker",
			Name:      "handled_task_total",
			Help:      "Total number of worker handled tasks.",
		}, []string{"name"})
	WorkerPendingTaskTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "worker",
			Name:      "pending_task_total",
			Help:      "Current worker pending + running tasks.",
		}, []string{"name"})
	WorkerTaskDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "worker",
			Name:      "task_duration_seconds",
			Help:      "Bucketed histogram of worker tasks duration seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
		}, []string{"type"})
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
	prometheus.MustRegister(ThreadCPUSecondsTotal)
	prometheus.MustRegister(RaftstoreRegionCount)
	prometheus.MustRegister(EngineSizeBytes)
	prometheus.MustRegister(EngineFlowBytes)
	prometheus.MustRegister(GrpcMsgDurationSeconds)
	prometheus.MustRegister(GrpcMsgFailTotal)
	prometheus.MustRegister(ServerGrpcReqBatchSize)
	prometheus.MustRegister(ServerGrpcRespBatchSize)
	prometheus.MustRegister(ServerRaftMessageBatchSize)
	prometheus.MustRegister(RegionWrittenKeys)
	prometheus.MustRegister(RegionWrittenBytes)
	prometheus.MustRegister(RegionReadKeys)
	prometheus.MustRegister(RegionReadBytes)
	prometheus.MustRegister(WorkerHandledTaskTotal)
	prometheus.MustRegister(WorkerPendingTaskTotal)
	prometheus.MustRegister(WorkerTaskDurationSeconds)
	http.Handle("/metrics", promhttp.Handler())
}
