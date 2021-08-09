package raftstore

import (
	"github.com/ngaut/unistore/engine"
	"github.com/ngaut/unistore/enginepb"
	"github.com/ngaut/unistore/raftengine"
	"github.com/ngaut/unistore/tikv/raftstore/raftlog"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/log"
	"io"
	"math"
)

type RecoverHandler struct {
	raftEngine    *raftengine.Engine
	storeID       uint64
	regionHandler *regionTaskHandler
}

func NewRecoverHandler(raftEngine *raftengine.Engine) (*RecoverHandler, error) {
	storeIdent, err := loadStoreIdent(raftEngine)
	if err != nil {
		return nil, err
	}
	if storeIdent == nil {
		return nil, nil
	}
	return &RecoverHandler{
		raftEngine: raftEngine,
		storeID:    storeIdent.StoreId,
	}, nil
}

func (h *RecoverHandler) Recover(kv *engine.Engine, shard *engine.Shard, meta *engine.ShardMeta) error {
	log.S().Infof("recover region:%d ver:%d", shard.ID, shard.Ver)
	aCtx := &applyContext{
		wb:      NewKVWriteBatch(kv),
		engines: &Engines{kv: kv, raft: h.raftEngine},
		execCtx: &applyExecContext{},
	}
	val, ok := shard.RecoverGetProperty(applyStateKey)
	if !ok {
		return errors.New("no applyState")
	}
	regionMeta, committedIdx, err1 := h.loadRegionMeta(shard.ID, shard.Ver)
	if err1 != nil {
		return errors.AddStack(err1)
	}
	var fromApplyState applyState
	fromApplyState.Unmarshal(val)
	lowIdx := fromApplyState.appliedIndex + 1
	highIdx := committedIdx
	entries, _, err1 := fetchEntriesTo(h.raftEngine, shard.ID, lowIdx, highIdx+1, math.MaxUint64, nil)
	if err1 != nil {
		return errors.AddStack(err1)
	}
	peer := findPeer(regionMeta, h.storeID)
	applier := &applier{
		peer:       peer,
		region:     regionMeta,
		applyState: fromApplyState,
		lockCache:  map[string][]byte{},
		snap:       kv.NewSnapAccess(shard),
	}
	defer applier.snap.Discard()
	for i := range entries {
		e := entries[i]
		if len(e.Data) == 0 || e.EntryType != eraftpb.EntryType_EntryNormal {
			continue
		}
		rlog := raftlog.DecodeLog(e.Data)
		if cmdReq := rlog.GetRaftCmdRequest(); cmdReq != nil {
			if cmdReq.GetAdminRequest().GetSplits() != nil {
				// We are recovering an parent shard, the split is the last command, we can skip it and return now.
				return nil
			}
			err := h.executeAdminRequest(applier, cmdReq)
			if err != nil {
				return err
			}
			continue
		}
		if rlog.Epoch().Ver() != shard.Ver {
			continue
		}
		var cl *raftlog.CustomRaftLog
		cl, ok = rlog.(*raftlog.CustomRaftLog)
		if !ok {
			// must be delete range request. TODO: handle it in the future.
			continue
		}
		aCtx.execCtx.applyState = applier.applyState
		aCtx.execCtx.index = e.Index
		aCtx.execCtx.term = e.Term
		if raftlog.IsBackgroundChangeSet(cl.Data) {
			cs, err := cl.GetShardChangeSet()
			if err != nil {
				return err
			}
			cs.Sequence = e.Index
			var rejected bool
			if meta.SplitStage >= enginepb.SplitStage_PRE_SPLIT && cs.Compaction != nil {
				cs.Compaction.Conflicted = true
				rejected = true
			}
			if rejected || !meta.IsDuplicatedChangeSet(cs) {
				// We don't have a background region worker now, should do it synchronously.
				err = kv.ApplyChangeSet(cs)
				if err != nil {
					return err
				}
			}
		} else {
			applier.execCustomLog(aCtx, cl)
		}
		applier.applyState.appliedIndex = e.Index
		applier.applyState.appliedIndexTerm = e.Term
	}
	newState := fromApplyState
	newState.appliedIndex = highIdx
	shard.RecoverSetProperty(applyStateKey, newState.Marshal())
	return nil
}

func (h *RecoverHandler) loadRegionMeta(id, ver uint64) (region *metapb.Region, committedIdx uint64, err error) {
	err = h.raftEngine.IterateRegionStates(id, true, func(key, val []byte) error {
		if key[0] != RegionMetaKeyByte {
			return nil
		}
		metaVer, _ := ParseRegionStateKey(key)
		if metaVer != ver {
			return nil
		}
		state := new(raft_serverpb.RegionLocalState)
		err1 := state.Unmarshal(val)
		if err1 != nil {
			return err1
		}
		region = state.Region
		return io.EOF
	})
	if err != io.EOF {
		return nil, 0, err
	}
	val := h.raftEngine.GetState(region.GetId(), RaftStateKey(region.RegionEpoch.Version))
	y.Assert(len(val) > 0)
	var raftState raftState
	raftState.Unmarshal(val)
	committedIdx = raftState.commit
	return region, committedIdx, nil
}

func (h *RecoverHandler) executeAdminRequest(a *applier, cmdReq *raft_cmdpb.RaftCmdRequest) error {
	adminReq := cmdReq.AdminRequest
	if adminReq.ChangePeer != nil {
		_, _, err := a.execChangePeer(adminReq)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *RecoverHandler) IterateMeta(fn func(meta *enginepb.ChangeSet) error) error {
	if h == nil {
		return nil
	}
	err := h.raftEngine.IterateAllStates(false, func(regionID uint64, key, val []byte) error {
		if key[0] == KVEngineMetaKeyByte {
			cs := new(enginepb.ChangeSet)
			err := cs.Unmarshal(val)
			if err != nil {
				return err
			}
			err = fn(cs)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}
