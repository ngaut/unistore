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
	RaftCommandDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "worker",
			Name:      "command_duration_seconds",
			Help:      "Bucketed histogram of raft command duration seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20),
		}, []string{"type"})
	SendToProposeWaitTimeDurationHistogram    = RaftCommandDurationSeconds.WithLabelValues("send_to_propose")
	ProposeToCommitWaitTimeDurationHistogram  = RaftCommandDurationSeconds.WithLabelValues("propose_to_commit")
	CommitToCallbackWaitTimeDurationHistogram = RaftCommandDurationSeconds.WithLabelValues("commit_to_callback")
	PeerRaftProcessDuration                   = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "raftstore",
			Name:      "raft_process_duration_secs",
			Help:      "Bucketed histogram of peer processing raft duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		}, []string{"type"})
	RaftstoreApplyProposal = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "raftstore",
			Name:      "apply_proposal",
			Help:      "The count of proposals sent by a region at once",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 20),
		})
	RequestWaitTimeDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "raftstore",
			Name:      "request_wait_time_duration_secs",
			Help:      "Bucketed histogram of request wait time duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})
	StoreApplyLogHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "raftstore",
			Name:      "apply_log_duration_seconds",
			Help:      "Bucketed histogram of peer applying log duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})
	PeerAppendLogHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "raftstore",
			Name:      "append_log_duration_seconds",
			Help:      "Bucketed histogram of peer appending log duration",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})
	ApplyTaskWaitTimeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "raftstore",
			Name:      "apply_wait_time_duration_secs",
			Help:      "Bucketed histogram of apply task wait time duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})
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
	CoprocessorDurationSeconds           = GrpcMsgDurationSeconds.WithLabelValues("coprocessor")
	KvBatchGetDurationSeconds            = GrpcMsgDurationSeconds.WithLabelValues("kv_batch_get")
	KvBatchRollbackDurationSeconds       = GrpcMsgDurationSeconds.WithLabelValues("kv_batch_rollback")
	KvCheckSecondaryLocksDurationSeconds = GrpcMsgDurationSeconds.WithLabelValues("kv_check_secondary_locks")
	KvCheckTxnStatusDurationSeconds      = GrpcMsgDurationSeconds.WithLabelValues("kv_check_txn_status")
	KvCleanupDurationSeconds             = GrpcMsgDurationSeconds.WithLabelValues("kv_cleanup")
	KvCommitDurationSeconds              = GrpcMsgDurationSeconds.WithLabelValues("kv_commit")
	KvDeleteRangeDurationSeconds         = GrpcMsgDurationSeconds.WithLabelValues("kv_delete_range")
	KvGCDurationSeconds                  = GrpcMsgDurationSeconds.WithLabelValues("kv_gc")
	KvGetDurationSeconds                 = GrpcMsgDurationSeconds.WithLabelValues("kv_get")
	PessimisticLockDurationSeconds       = GrpcMsgDurationSeconds.WithLabelValues("pessimistic_lock")
	PessimisticRollbackDurationSeconds   = GrpcMsgDurationSeconds.WithLabelValues("pessimistic_rollback")
	KvPrewriteDurationSeconds            = GrpcMsgDurationSeconds.WithLabelValues("kv_prewrite")
	KvResolveLockDurationSeconds         = GrpcMsgDurationSeconds.WithLabelValues("kv_resolve_lock")
	KvScanDurationSeconds                = GrpcMsgDurationSeconds.WithLabelValues("kv_scan")
	KvScanLockDurationSeconds            = GrpcMsgDurationSeconds.WithLabelValues("kv_scan_lock")
	TxnHeartBeatDurationSeconds          = GrpcMsgDurationSeconds.WithLabelValues("txn_heartbeat")
	MvccGetByKeyDurationSeconds          = GrpcMsgDurationSeconds.WithLabelValues("mvcc_get_by_key")
	MvccGetByStartTsDurationSeconds      = GrpcMsgDurationSeconds.WithLabelValues("mvcc_get_by_start_ts")
	SplitRegionDurationSeconds           = GrpcMsgDurationSeconds.WithLabelValues("split_region")
	RawGetDurationSeconds                = GrpcMsgDurationSeconds.WithLabelValues("raw_get")
	RawBatchGetDurationSeconds           = GrpcMsgDurationSeconds.WithLabelValues("raw_batch_get")
	RawPutDurationSeconds                = GrpcMsgDurationSeconds.WithLabelValues("raw_put")
	RawBatchPutDurationSeconds           = GrpcMsgDurationSeconds.WithLabelValues("raw_batch_put")
	RawDeleteDurationSeconds             = GrpcMsgDurationSeconds.WithLabelValues("raw_delete")
	RawBatchDeleteDurationSeconds        = GrpcMsgDurationSeconds.WithLabelValues("raw_batch_delete")
	RawScanDurationSeconds               = GrpcMsgDurationSeconds.WithLabelValues("raw_scan")
	RawDeleteRangeDurationSeconds        = GrpcMsgDurationSeconds.WithLabelValues("raw_delete_range")
	GrpcMsgFailTotal                     = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "grpc",
			Name:      "msg_fail_total",
			Help:      "Total number of handle grpc message failure",
		}, []string{"type"})
	CoprocessorFailTotal           = GrpcMsgFailTotal.WithLabelValues("coprocessor")
	KvBatchGetFailTotal            = GrpcMsgFailTotal.WithLabelValues("kv_batch_get")
	KvBatchRollbackFailTotal       = GrpcMsgFailTotal.WithLabelValues("kv_batch_rollback")
	KvCheckSecondaryLocksFailTotal = GrpcMsgFailTotal.WithLabelValues("kv_check_secondary_locks")
	KvCheckTxnStatusFailTotal      = GrpcMsgFailTotal.WithLabelValues("kv_check_txn_status")
	KvCleanupFailTotal             = GrpcMsgFailTotal.WithLabelValues("kv_cleanup")
	KvCommitFailTotal              = GrpcMsgFailTotal.WithLabelValues("kv_commit")
	KvDeleteRangeFailTotal         = GrpcMsgFailTotal.WithLabelValues("kv_delete_range")
	KvGCFailTotal                  = GrpcMsgFailTotal.WithLabelValues("kv_gc")
	KvGetFailTotal                 = GrpcMsgFailTotal.WithLabelValues("kv_get")
	PessimisticLockFailTotal       = GrpcMsgFailTotal.WithLabelValues("pessimistic_lock")
	PessimisticRollbackFailTotal   = GrpcMsgFailTotal.WithLabelValues("pessimistic_rollback")
	KvPrewriteFailTotal            = GrpcMsgFailTotal.WithLabelValues("kv_prewrite")
	KvResolveLockFailTotal         = GrpcMsgFailTotal.WithLabelValues("kv_resolve_lock")
	KvScanFailTotal                = GrpcMsgFailTotal.WithLabelValues("kv_scan")
	KvScanLockFailTotal            = GrpcMsgFailTotal.WithLabelValues("kv_scan_lock")
	TxnHeartBeatFailTotal          = GrpcMsgFailTotal.WithLabelValues("txn_heartbeat")
	MvccGetByKeyFailTotal          = GrpcMsgFailTotal.WithLabelValues("mvcc_get_by_key")
	MvccGetByStartTsFailTotal      = GrpcMsgFailTotal.WithLabelValues("mvcc_get_by_start_ts")
	SplitRegionFailTotal           = GrpcMsgFailTotal.WithLabelValues("split_region")
	RawGetFailTotal                = GrpcMsgFailTotal.WithLabelValues("raw_get")
	RawBatchGetFailTotal           = GrpcMsgFailTotal.WithLabelValues("raw_batch_get")
	RawPutFailTotal                = GrpcMsgFailTotal.WithLabelValues("raw_put")
	RawBatchPutFailTotal           = GrpcMsgFailTotal.WithLabelValues("raw_batch_put")
	RawDeleteFailTotal             = GrpcMsgFailTotal.WithLabelValues("raw_delete")
	RawBatchDeleteFailTotal        = GrpcMsgFailTotal.WithLabelValues("raw_batch_delete")
	RawScanFailTotal               = GrpcMsgFailTotal.WithLabelValues("raw_scan")
	RawDeleteRangeFailTotal        = GrpcMsgFailTotal.WithLabelValues("raw_delete_range")
	ServerGrpcReqBatchSize         = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "server",
			Name:      "grpc_req_batch_size",
			Help:      "grpc batch size of gRPC requests",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
		})
	ServerGrpcLightweightReqBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "server",
			Name:      "grpc_lightweight_req_batch_size",
			Help:      "grpc batch size of gRPC lightweight requests",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
		})
	ServerGrpcRespBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "server",
			Name:      "grpc_resp_batch_size",
			Help:      "grpc batch size of gRPC responses",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
		})
	ServerRaftMessageBatchSize = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "server",
			Name:      "raft_message_batch_size",
			Help:      "Raft messages batch size",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
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
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20),
		}, []string{"type"})
	WorkerLoopDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "worker",
			Name:      "loop_duration_seconds",
			Help:      "Bucketed histogram of worker loop duration seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20),
		}, []string{"type"})
	RaftWorkerLoopDurationHistogram  = WorkerLoopDurationSeconds.WithLabelValues("raft_worker")
	RaftWorkerMessageDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "worker",
			Name:      "message_duration_seconds",
			Help:      "Bucketed histogram of raft message duration seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 20),
		}, []string{"type"})
	WaitMessageDurationHistogram      = RaftWorkerMessageDurationSeconds.WithLabelValues("wait_message")
	ReceiveMessageDurationHistogram   = RaftWorkerMessageDurationSeconds.WithLabelValues("receive_message")
	ProcessRegionDurationHistogram    = RaftWorkerMessageDurationSeconds.WithLabelValues("process_region")
	PersistStateDurationHistogram     = RaftWorkerMessageDurationSeconds.WithLabelValues("persist_state")
	PostPersistStateDurationHistogram = RaftWorkerMessageDurationSeconds.WithLabelValues("post_persist_state")
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
	prometheus.MustRegister(RaftCommandDurationSeconds)
	prometheus.MustRegister(PeerRaftProcessDuration)
	prometheus.MustRegister(RaftstoreApplyProposal)
	prometheus.MustRegister(RequestWaitTimeDurationHistogram)
	prometheus.MustRegister(StoreApplyLogHistogram)
	prometheus.MustRegister(PeerAppendLogHistogram)
	prometheus.MustRegister(ApplyTaskWaitTimeHistogram)
	prometheus.MustRegister(EngineSizeBytes)
	prometheus.MustRegister(EngineFlowBytes)
	prometheus.MustRegister(GrpcMsgDurationSeconds)
	prometheus.MustRegister(GrpcMsgFailTotal)
	prometheus.MustRegister(ServerGrpcReqBatchSize)
	prometheus.MustRegister(ServerGrpcRespBatchSize)
	prometheus.MustRegister(ServerGrpcLightweightReqBatchSize)
	prometheus.MustRegister(ServerRaftMessageBatchSize)
	prometheus.MustRegister(RegionWrittenKeys)
	prometheus.MustRegister(RegionWrittenBytes)
	prometheus.MustRegister(RegionReadKeys)
	prometheus.MustRegister(RegionReadBytes)
	prometheus.MustRegister(WorkerHandledTaskTotal)
	prometheus.MustRegister(WorkerPendingTaskTotal)
	prometheus.MustRegister(WorkerTaskDurationSeconds)
	prometheus.MustRegister(WorkerLoopDurationSeconds)
	prometheus.MustRegister(RaftWorkerMessageDurationSeconds)
	http.Handle("/metrics", promhttp.Handler())
}
