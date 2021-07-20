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
	"encoding/binary"
	"github.com/ngaut/unistore/engine"
	"github.com/ngaut/unistore/enginepb"
	"github.com/ngaut/unistore/tikv/raftstore/raftlog"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/unistore/config"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
)

type taskType int64

const (
	taskTypeStop           taskType = 0
	taskTypeRaftLogGC      taskType = 1
	taskTypeSplitCheck     taskType = 2
	taskTypeComputeHash    taskType = 3
	taskTypeHalfSplitCheck taskType = 4

	taskTypePDAskBatchSplit    taskType = 102
	taskTypePDHeartbeat        taskType = 103
	taskTypePDStoreHeartbeat   taskType = 104
	taskTypePDReportBatchSplit taskType = 105
	taskTypePDValidatePeer     taskType = 106
	taskTypePDReadStats        taskType = 107
	taskTypePDDestroyPeer      taskType = 108

	taskTypeRegionApply taskType = 402
	/// Destroy data between [start_key, end_key).
	///
	/// The deletion may and may not succeed.
	taskTypeRegionDestroy        taskType = 403
	taskTypeRegionApplyChangeSet taskType = 404
	taskTypeRecoverSplit         taskType = 405
	taskTypeFinishSplit          taskType = 406
)

type task struct {
	tp   taskType
	data interface{}
}

type regionTask struct {
	region   *metapb.Region
	notifier chan<- *eraftpb.Snapshot
	status   *JobStatus
	snapData *snapData
	startKey []byte
	endKey   []byte
	redoIdx  uint64

	change *enginepb.ChangeSet
	peer   *metapb.Peer

	waitMsg *MsgWaitFollowerSplitFiles
}

type splitCheckTask struct {
	region *metapb.Region
	peer   *metapb.Peer
}

type computeHashTask struct {
	index  uint64
	region *metapb.Region
	snap   *engine.SnapAccess
}

type pdAskBatchSplitTask struct {
	region    *metapb.Region
	splitKeys [][]byte
	peer      *metapb.Peer
	// If true, right Region derives origin region_id.
	rightDerive bool
	callback    *Callback
}

type pdRegionHeartbeatTask struct {
	region          *metapb.Region
	peer            *metapb.Peer
	downPeers       []*pdpb.PeerStats
	pendingPeers    []*metapb.Peer
	writtenBytes    uint64
	writtenKeys     uint64
	approximateSize uint64
	approximateKeys uint64
}

type pdStoreHeartbeatTask struct {
	stats    *pdpb.StoreStats
	kv       *engine.Engine
	path     string
	capacity uint64
}

type pdReportBatchSplitTask struct {
	regions []*metapb.Region
}

type pdValidatePeerTask struct {
	region      *metapb.Region
	peer        *metapb.Peer
	mergeSource *uint64
}

type readStats map[uint64]flowStats

type pdDestroyPeerTask struct {
	regionID uint64
}

type flowStats struct {
	readBytes uint64
	readKeys  uint64
}

type worker struct {
	name     string
	sender   chan<- task
	receiver <-chan task
	closeCh  chan struct{}
	wg       *sync.WaitGroup
}

type taskHandler interface {
	handle(t task)
}

type starter interface {
	start()
}

func (w *worker) start(handler taskHandler) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if s, ok := handler.(starter); ok {
			s.start()
		}
		for {
			task := <-w.receiver
			if task.tp == taskTypeStop {
				return
			}
			handler.handle(task)
		}
	}()
}

func (w *worker) stop() {
	w.sender <- task{tp: taskTypeStop}
}

const defaultWorkerCapacity = 128

func newWorker(name string, wg *sync.WaitGroup) *worker {
	ch := make(chan task, defaultWorkerCapacity)
	return &worker{
		sender:   (chan<- task)(ch),
		receiver: (<-chan task)(ch),
		name:     name,
		wg:       wg,
	}
}

type splitCheckHandler struct {
	kv     *engine.Engine
	router *router
	config *splitCheckConfig
}

func newSplitCheckRunner(kv *engine.Engine, router *router, config *splitCheckConfig) *splitCheckHandler {
	runner := &splitCheckHandler{
		kv:     kv,
		router: router,
		config: config,
	}
	return runner
}

/// run checks a region with split checkers to produce split keys and generates split admin command.
func (r *splitCheckHandler) handle(t task) {
	spCheckTask := t.data.(*splitCheckTask)
	region := spCheckTask.region
	regionId := region.Id
	keys := r.kv.GetSplitSuggestion(regionId, int64(r.config.RegionMaxSize))
	if len(keys) != 0 {
		log.S().Infof("split %d:%d by checker size:%d", region.Id, region.RegionEpoch.Version, r.config.RegionMaxSize)
		_, err := splitEngineAndRegion(r.router, r.kv, spCheckTask.peer, region, keys)
		if err != nil {
			log.Warn("failed to send check result", zap.Uint64("region id", regionId), zap.Error(err))
		}
	}
}

// splitEngineAndRegion execute the complete procedure to split a region.
// 1. execute PreSplit on raft command.
// 2. Split the engine files.
// 3. Split the region.
func splitEngineAndRegion(router *router, eng *engine.Engine, peer *metapb.Peer, region *metapb.Region, keys [][]byte) (*Callback, error) {
	// Make sure the region doesn't has parent before split.
	err := preSplitRegion(router, eng, peer, region, keys)
	if err != nil {
		return nil, errors.Wrap(err, "failed to pre-split region")
	}
	err = splitShardFiles(router, eng, peer, region)
	if err != nil {
		return nil, errors.Wrap(err, "failed to split files")
	}
	cb := NewCallback()
	msg := &MsgWaitFollowerSplitFiles{
		SplitKeys: keys,
		Callback:  cb,
	}
	err = router.send(region.Id, NewPeerMsg(MsgTypeWaitFollowerSplitFiles, region.Id, msg))
	if err != nil {
		return nil, err
	}
	return cb, nil
}

func preSplitRegion(router *router, eng *engine.Engine, peer *metapb.Peer, region *metapb.Region, rawKeys [][]byte) error {
	shard := eng.GetShard(region.Id)
	for {
		if shard.IsInitialFlushed() {
			break
		}
		time.Sleep(time.Second)
		log.S().Infof("shard %d:%d wait for initial flush", shard.ID, shard.Ver)
	}
	if shard.GetSplitStage() != enginepb.SplitStage_INITIAL {
		return errors.New("wrong split stage " + shard.GetSplitStage().String())
	}
	header := raftlog.CustomHeader{
		RegionID: region.Id,
		Epoch:    raftlog.NewEpoch(region.RegionEpoch.Version, region.RegionEpoch.ConfVer),
		PeerID:   peer.Id,
		StoreID:  peer.StoreId,
	}
	builder := raftlog.NewBuilder(header)
	preSplitCS := &enginepb.ChangeSet{
		ShardID:  region.Id,
		ShardVer: region.RegionEpoch.Version,
		PreSplit: &enginepb.PreSplit{
			Keys: rawKeys,
		},
		Stage: enginepb.SplitStage_PRE_SPLIT,
	}
	builder.SetChangeSet(preSplitCS)
	cb := NewCallback()
	cmd := &MsgRaftCmd{
		SendTime: time.Now(),
		Callback: cb,
		Request:  builder.Build(),
	}
	router.sendRaftCommand(cmd)
	resp := cb.Wait()
	if resp.GetHeader().GetError() != nil {
		return errors.New(resp.Header.Error.Message)
	}
	return nil
}

func splitShardFiles(router *router, eng *engine.Engine, peer *metapb.Peer, region *metapb.Region) error {
	change, err := eng.SplitShardFiles(region.Id, region.RegionEpoch.Version)
	if err != nil {
		return err
	}
	header := raftlog.CustomHeader{
		RegionID: region.Id,
		Epoch:    raftlog.NewEpoch(region.RegionEpoch.Version, region.RegionEpoch.ConfVer),
		PeerID:   peer.Id,
		StoreID:  peer.StoreId,
	}
	builder := raftlog.NewBuilder(header)
	builder.SetChangeSet(change)
	cb := NewCallback()
	cmd := &MsgRaftCmd{
		SendTime: time.Now(),
		Callback: cb,
		Request:  builder.Build(),
	}
	router.sendRaftCommand(cmd)
	resp := cb.Wait()
	if resp.GetHeader().GetError() != nil {
		return errors.New(resp.Header.Error.Message)
	}
	return nil
}

func finishSplit(router *router, region *metapb.Region, rawKeys [][]byte) ([]*metapb.Region, error) {
	encodedKeys := make([][]byte, len(rawKeys))
	for i := 0; i < len(rawKeys); i++ {
		encodedKeys[i] = codec.EncodeBytes(nil, rawKeys[i])
	}
	splitRegionCB := NewCallback()
	splitRegionMsg := &MsgSplitRegion{
		RegionEpoch: region.RegionEpoch,
		SplitKeys:   encodedKeys,
		Callback:    splitRegionCB,
	}
	err := router.send(region.Id, Msg{Type: MsgTypeSplitRegion, Data: splitRegionMsg})
	if err != nil {
		return nil, err
	}
	resp := splitRegionCB.Wait()
	if resp.GetHeader().GetError() != nil {
		return nil, errors.New(resp.Header.Error.Message)
	}
	return resp.GetAdminResponse().GetSplits().GetRegions(), nil
}

type stalePeerInfo struct {
	data []byte
}

func newStalePeerInfo(regionId uint64, endKey []byte, timeout time.Time) stalePeerInfo {
	s := stalePeerInfo{data: make([]byte, 16+len(endKey))}
	s.setRegionId(regionId)
	s.setTimeout(timeout)
	s.setEndKey(endKey)
	return s
}

func (s stalePeerInfo) regionId() uint64 {
	return binary.LittleEndian.Uint64(s.data[:8])
}

func (s stalePeerInfo) timeout() time.Time {
	return time.Unix(0, int64(binary.LittleEndian.Uint64(s.data[8:16])))
}

func (s stalePeerInfo) endKey() []byte {
	return s.data[16:]
}

func (s stalePeerInfo) setRegionId(regionId uint64) {
	binary.LittleEndian.PutUint64(s.data[:8], regionId)
}

func (s stalePeerInfo) setTimeout(timeout time.Time) {
	binary.LittleEndian.PutUint64(s.data[8:16], uint64(timeout.UnixNano()))
}

func (s stalePeerInfo) setEndKey(endKey []byte) {
	copy(s.data[16:], endKey)
}

type regionTaskHandler struct {
	kv     *engine.Engine
	conf   *config.Config
	router *router
}

func newRegionTaskHandler(conf *config.Config, engines *Engines, router *router) *regionTaskHandler {
	return &regionTaskHandler{
		conf:   conf,
		kv:     engines.kv,
		router: router,
	}
}

// handlePendingApplies tries to apply pending tasks if there is some.
func (r *regionTaskHandler) handleApply(task *regionTask) {
	atomic.StoreUint32(task.status, JobStatus_Running)
	snapData := task.snapData
	inTree := &engine.IngestTree{
		ChangeSet: snapData.changeSet,
		Passive:   true,
	}
	err := r.kv.Ingest(inTree)
	if err != nil {
		log.Error("update region status failed", zap.Error(err))
		atomic.StoreUint32(task.status, JobStatus_Failed)
	} else {
		atomic.StoreUint32(task.status, JobStatus_Finished)
	}
}

func (r *regionTaskHandler) handle(t task) {
	switch t.tp {
	case taskTypeRegionApply:
		// To make sure applying snapshots in order.
		task := t.data.(*regionTask)
		r.handleApply(task)
	case taskTypeRegionDestroy:
		// We don't need to delay the range deletion because DeleteRange operation
		// doesn't affect the existing badger.Snapshot
		regionTask := t.data.(*regionTask)
		err := r.kv.RemoveShard(regionTask.region.Id, true)
		if err != nil {
			log.Error("failed to destroy region", zap.Error(err))
		}
	case taskTypeRegionApplyChangeSet:
		regionTask := t.data.(*regionTask)
		changeSet := regionTask.change
		kv := r.kv
		var changeSetTp string
		if changeSet.Flush != nil {
			changeSetTp = "flush"
		} else if changeSet.Compaction != nil {
			changeSetTp = "compaction"
		} else if changeSet.SplitFiles != nil {
			changeSetTp = "split_files"
		}
		log.S().Infof("shard %d:%d apply change set %s stage %s seq %d", changeSet.ShardID, changeSet.ShardVer, changeSetTp, changeSet.Stage, changeSet.Sequence)
		err := kv.ApplyChangeSet(changeSet)
		if err != nil {
			log.Error("failed to apply passive change set", zap.Error(err), zap.String("changeSet", changeSet.String()))
		}
		_ = r.router.send(changeSet.ShardID, NewPeerMsg(MsgTypeApplyChangeSetResult, changeSet.ShardID, &MsgApplyChangeSetResult{
			change: changeSet,
			err:    err,
		}))
	case taskTypeRecoverSplit:
		regionTask := t.data.(*regionTask)
		err := r.handleRecoverSplit(regionTask.region, regionTask.peer)
		if err != nil {
			log.S().Errorf("region %d:%d failed to recover split err %s", regionTask.region.Id, regionTask.region.RegionEpoch.Version, err.Error())
		}
	case taskTypeFinishSplit:
		regionTask := t.data.(*regionTask)
		waitMsg := regionTask.waitMsg
		regions, err := finishSplit(r.router, regionTask.region, waitMsg.SplitKeys)
		if err != nil {
			waitMsg.Callback.Done(ErrResp(err))
		} else {
			waitMsg.Callback.Done(&raft_cmdpb.RaftCmdResponse{
				AdminResponse: &raft_cmdpb.AdminResponse{
					Splits: &raft_cmdpb.BatchSplitResponse{Regions: regions},
				},
			})
		}
	}
}

func (r *regionTaskHandler) shutdown() {
	// todo, currently it is a a place holder.
}

func (r *regionTaskHandler) handleRecoverSplit(region *metapb.Region, peer *metapb.Peer) error {
	shard := r.kv.GetShard(region.Id)
	switch shard.GetSplitStage() {
	case enginepb.SplitStage_PRE_SPLIT, enginepb.SplitStage_PRE_SPLIT_FLUSH_DONE:
		err := splitShardFiles(r.router, r.kv, peer, region)
		if err != nil {
			return err
		}
	}
	cb := NewCallback()
	msg := &MsgWaitFollowerSplitFiles{
		SplitKeys: shard.GetPreSplitKeys(),
		Callback:  cb,
	}
	return r.router.send(region.Id, NewPeerMsg(MsgTypeWaitFollowerSplitFiles, region.Id, msg))
}

type computeHashTaskHandler struct {
	router *router
}

func (r *computeHashTaskHandler) handle(t task) {
	// TODO: stub
}
