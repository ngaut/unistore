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
	"time"

	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/metrics"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/codec"
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
	taskTypeRejectChangeSet      taskType = 407
)

type task struct {
	tp   taskType
	data interface{}
}

type regionTask struct {
	region   *metapb.Region
	notifier chan<- *eraftpb.Snapshot
	snapData *snapData
	startKey []byte
	endKey   []byte

	change *enginepb.ChangeSet
	peer   *metapb.Peer
	wg     sync.WaitGroup
	err    error

	waitMsg *MsgWaitFollowerSplitFiles

	// For recover split.
	stage     enginepb.SplitStage
	splitKeys [][]byte
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
	stats       *pdpb.StoreStats
	kv          *engine.Engine
	path        string
	capacity    uint64
	leaderCount uint64
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
			metrics.WorkerPendingTaskTotal.WithLabelValues(w.name).Set(float64(len(w.receiver) + 1))
			handler.handle(task)
			metrics.WorkerHandledTaskTotal.WithLabelValues(w.name).Inc()
			metrics.WorkerPendingTaskTotal.WithLabelValues(w.name).Set(float64(len(w.receiver)))
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
		log.S().Infof("split region %d:%d by checker size:%d", region.Id, region.RegionEpoch.Version, r.config.RegionMaxSize)
		_, err := splitEngineAndRegion(r.router, r.kv, spCheckTask.peer, region, keys)
		if err != nil {
			log.S().Warnf("region %d:%d failed to send check result err: %s", region.Id, region.RegionEpoch.Version, err.Error())
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
		log.S().Warnf("shard %d:%d failed to pre-split region err: %s", region.Id, region.RegionEpoch.Version, err.Error())
		return nil, errors.Wrap(err, "failed to pre-split region")
	}
	err = splitShardFiles(router, eng, peer, region)
	if err != nil {
		log.S().Warnf("shard %d:%d failed to split files err: %s", region.Id, region.RegionEpoch.Version, err.Error())
		return nil, errors.Wrap(err, "failed to split files")
	}
	log.S().Infof("shard %d:%d send a msg to wait for followers to finish splitting files", region.Id, region.RegionEpoch.Version)
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
		log.S().Warnf("shard %d:%d wrong split stage %s", shard.ID, shard.Ver, shard.GetSplitStage().String())
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
		log.S().Warnf("shard %d:%d split shard files err: %s", region.Id, region.RegionEpoch.Version, err.Error())
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
	for {
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
			if resp.GetHeader().GetError().EpochNotMatch != nil {
				epochNotMatch := resp.GetHeader().GetError().EpochNotMatch
				var r *metapb.Region
				if len(epochNotMatch.CurrentRegions) > 0 {
					for _, cr := range epochNotMatch.CurrentRegions {
						if cr.Id == region.Id {
							r = cr
							break
						}
					}
				}
				if r != nil && r.RegionEpoch.Version == region.RegionEpoch.Version {
					log.S().Warnf("region %d:%d leader finish split err: %s", region.Id, region.RegionEpoch.Version, resp.GetHeader().GetError().Message)
					region = r
					continue
				}
			} else if resp.GetHeader().GetError().Message == errPendingConfChange.Error() {
				log.S().Warnf("region %d:%d leader finish split err: %s", region.Id, region.RegionEpoch.Version, errPendingConfChange.Error())
				time.Sleep(time.Millisecond * 100)
				continue
			}
			return nil, errors.New(resp.Header.Error.Message)
		}
		log.S().Infof("region %d:%d leader finish split successfully", region.Id, region.RegionEpoch.Version)
		return resp.GetAdminResponse().GetSplits().GetRegions(), nil
	}
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

type changeSetKey struct {
	regionID uint64
	sequence uint64
}

type regionTaskHandler struct {
	kv         *engine.Engine
	conf       *config.Config
	router     *router
	applySched chan<- task
	rejects    map[changeSetKey]*enginepb.ChangeSet
}

func newRegionTaskHandler(conf *config.Config, engines *Engines, router *router, applySched chan<- task) *regionTaskHandler {
	return &regionTaskHandler{
		conf:       conf,
		kv:         engines.kv,
		router:     router,
		applySched: applySched,
		rejects:    map[changeSetKey]*enginepb.ChangeSet{},
	}
}

// handlePendingApplies tries to apply pending tasks if there is some.
func (r *regionTaskHandler) prepareSnapshotResources(task *regionTask) {
	task.wg.Add(1)
	r.kv.ScheduleResources(func() {
		task.err = r.kv.PrepareResources(task.snapData.changeSet)
		task.wg.Done()
	})
}

func (r *regionTaskHandler) handle(t task) {
	regionTask := t.data.(*regionTask)
	switch t.tp {
	case taskTypeRegionApply:
		// To make sure applying snapshots in order.
		r.prepareSnapshotResources(regionTask)
		r.applySched <- t
	case taskTypeRegionDestroy:
		// We don't need to delay the range deletion because DeleteRange operation
		// doesn't affect the existing badger.Snapshot
		r.applySched <- t
	case taskTypeRejectChangeSet:
		r.handleRejectChangeSet(regionTask)
	case taskTypeRegionApplyChangeSet:
		if duplicated := r.prepareChangeSetResources(regionTask); !duplicated {
			r.applySched <- t
		}
	case taskTypeRecoverSplit:
		r.applySched <- t
	case taskTypeFinishSplit:
		r.applySched <- t
	}
}

func (r *regionTaskHandler) shutdown() {
	// todo, currently it is a a place holder.
}

func (r *regionTaskHandler) handleRejectChangeSet(task *regionTask) error {
	rejectKey := changeSetKey{task.region.Id, task.change.Sequence}
	r.rejects[rejectKey] = task.change
	return nil
}

func (r *regionTaskHandler) prepareChangeSetResources(task *regionTask) (duplicated bool) {
	changeSet := task.change
	kv := r.kv
	var changeSetTp string
	if changeSet.Flush != nil {
		changeSetTp = "flush"
	} else if changeSet.Compaction != nil {
		changeSetTp = "compaction"
	} else if changeSet.SplitFiles != nil {
		changeSetTp = "split_files"
	}
	key := changeSetKey{task.region.Id, task.change.Sequence}
	metaChange, rejected := r.rejects[key]
	if rejected {
		delete(r.rejects, key)
		shard := kv.GetShard(changeSet.ShardID)
		log.S().Warnf("shard %d:%d reject change set %s stage %s seq %d, initial flushed %t",
			changeSet.ShardID, changeSet.ShardVer, changeSetTp, changeSet.Stage, changeSet.Sequence, shard.IsInitialFlushed())
		if metaChange.Compaction == nil || !metaChange.Compaction.Conflicted {
			return true
		}
		changeSet.Compaction.Conflicted = true
		return false
	}
	log.S().Infof("shard %d:%d apply change set %s stage %s seq %d",
		changeSet.ShardID, changeSet.ShardVer, changeSetTp, changeSet.Stage, changeSet.Sequence)
	task.wg.Add(1)
	r.kv.ScheduleResources(func() {
		task.err = r.kv.PrepareResources(changeSet)
		task.wg.Done()
	})
	return false
}

type regionApplyTaskHandler struct {
	kv     *engine.Engine
	router *router
}

func newRegionApplyTaskHandler(engines *Engines, router *router) *regionApplyTaskHandler {
	return &regionApplyTaskHandler{
		kv:     engines.kv,
		router: router,
	}
}

func (r *regionApplyTaskHandler) handle(t task) {
	regionTask := t.data.(*regionTask)
	regionTask.wg.Wait()
	switch t.tp {
	case taskTypeRegionApply:
		// To make sure applying snapshots in order.
		err := r.handleApply(regionTask)
		_ = r.router.send(regionTask.region.Id, NewPeerMsg(MsgTypeApplyChangeSetResult, regionTask.region.Id, &MsgApplyChangeSetResult{
			change: regionTask.snapData.changeSet,
			err:    err,
		}))
	case taskTypeRegionDestroy:
		// We don't need to delay the range deletion because DeleteRange operation
		// doesn't affect the existing badger.Snapshot
		err := r.kv.RemoveShard(regionTask.region.Id, true)
		if err != nil {
			log.S().Errorf("region %d:%d failed to destroy region err %s", regionTask.region.Id, regionTask.region.RegionEpoch.Version, err.Error())
		}
	case taskTypeRegionApplyChangeSet:
		err := r.handleApplyChangeSet(regionTask)
		_ = r.router.send(regionTask.region.Id, NewPeerMsg(MsgTypeApplyChangeSetResult, regionTask.region.Id, &MsgApplyChangeSetResult{
			change: regionTask.change,
			err:    err,
		}))
	case taskTypeRecoverSplit:
		err := r.handleRecoverSplit(regionTask)
		if err != nil {
			log.S().Errorf("region %d:%d failed to recover split err %s", regionTask.region.Id, regionTask.region.RegionEpoch.Version, err.Error())
		}
	case taskTypeFinishSplit:
		waitMsg := regionTask.waitMsg
		regions, err := finishSplit(r.router, regionTask.region, waitMsg.SplitKeys)
		if err != nil {
			log.S().Errorf("region %d:%d failed to finish split err %s", regionTask.region.Id, regionTask.region.RegionEpoch.Version, err.Error())
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

func (r *regionApplyTaskHandler) handleRecoverSplit(task *regionTask) error {
	log.S().Infof("shard %d:%d handle recover split", task.region.Id, task.region.RegionEpoch.Version)
	switch task.stage {
	case enginepb.SplitStage_PRE_SPLIT:
		log.S().Panicf("shard %d:%d the region worker will be blocked", task.region.Id, task.region.RegionEpoch.Version)
	case enginepb.SplitStage_PRE_SPLIT_FLUSH_DONE:
		err := splitShardFiles(r.router, r.kv, task.peer, task.region)
		if err != nil {
			log.S().Warnf("shard %d:%d failed to split files err: %s", task.region.Id, task.region.RegionEpoch.Version, err.Error())
			return err
		}
	}
	log.S().Infof("shard %d:%d send a msg to wait for followers to finish splitting files", task.region.Id, task.region.RegionEpoch.Version)
	cb := NewCallback()
	msg := &MsgWaitFollowerSplitFiles{
		SplitKeys: task.splitKeys,
		Callback:  cb,
	}
	return r.router.send(task.region.Id, NewPeerMsg(MsgTypeWaitFollowerSplitFiles, task.region.Id, msg))
}

// handlePendingApplies tries to apply pending tasks if there is some.
func (r *regionApplyTaskHandler) handleApply(task *regionTask) error {
	changeSet := task.snapData.changeSet
	if task.err != nil {
		log.S().Errorf("failed to prepare snapshot resources err %s change set %s",
			task.err.Error(), changeSet.String())
		return task.err
	}
	inTree := &engine.IngestTree{
		ChangeSet: changeSet,
		Passive:   true,
	}
	return r.kv.Ingest(inTree)
}

func (r *regionApplyTaskHandler) handleApplyChangeSet(task *regionTask) error {
	changeSet := task.change
	if task.err != nil {
		log.S().Errorf("failed to prepare change set resources err %s change set %s",
			task.err.Error(), changeSet.String())
		return task.err
	}
	err := r.kv.ApplyChangeSet(changeSet)
	if err != nil {
		log.S().Errorf("failed to apply passive change set err %s change set %s",
			err.Error(), changeSet.String())
	}
	return err
}

type computeHashTaskHandler struct {
	router *router
}

func (r *computeHashTaskHandler) handle(t task) {
	// TODO: stub
}
