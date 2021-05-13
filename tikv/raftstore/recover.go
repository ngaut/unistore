package raftstore

import (
	"github.com/ngaut/unistore/sdb"
	"github.com/ngaut/unistore/sdbpb"
	"github.com/ngaut/unistore/tikv/raftstore/raftlog"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/log"
	"math"
)

type RecoverHandler struct {
	raftDB        *badger.DB
	storeID       uint64
	ctx           *applyContext
	regionHandler *regionTaskHandler
}

func NewRecoverHandler(raftDB *badger.DB) (*RecoverHandler, error) {
	storeIdent, err := loadStoreIdent(raftDB)
	if err != nil {
		return nil, err
	}
	if storeIdent == nil {
		return nil, nil
	}
	return &RecoverHandler{
		raftDB:  raftDB,
		storeID: storeIdent.StoreId,
	}, nil
}

func (h *RecoverHandler) Recover(db *sdb.DB, shard *sdb.Shard, meta *sdb.ShardMeta, toState *sdbpb.Properties) error {
	log.S().Infof("recover region:%d ver:%d", shard.ID, shard.Ver)
	if h.ctx == nil {
		h.ctx = &applyContext{wb: NewKVWriteBatch(db), engines: &Engines{kv: db, raft: h.raftDB}}
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
	if toState != nil {
		val, ok = sdb.GetShardProperty(applyStateKey, toState)
		if !ok {
			return errors.New("no applyState")
		}
		var toApplyState applyState
		toApplyState.Unmarshal(val)
		highIdx = toApplyState.appliedIndex
	}
	entries, _, err1 := fetchEntriesTo(h.raftDB, shard.ID, lowIdx, highIdx+1, math.MaxUint64, nil)
	if err1 != nil {
		return errors.AddStack(err1)
	}
	peer := findPeer(regionMeta, h.storeID)
	peerID := peer.Id
	applier := &applier{id: peerID, region: regionMeta, applyState: fromApplyState}
	for i := range entries {
		e := &entries[i]
		if len(e.Data) == 0 || e.EntryType != eraftpb.EntryType_EntryNormal {
			continue
		}
		rlog := raftlog.DecodeLog(e)
		if cmdReq := rlog.GetRaftCmdRequest(); cmdReq != nil {
			err := h.executeAdminRequest(applier, cmdReq)
			if err != nil {
				return err
			}
			continue
		}
		var cl *raftlog.CustomRaftLog
		cl, ok = rlog.(*raftlog.CustomRaftLog)
		if !ok {
			// must be delete range request. TODO: handle it in the future.
			continue
		}
		if cl.Type() == raftlog.TypeChangeSet {
			// We don't have a background region worker now, should do it synchronously.
			cs, err := cl.GetShardChangeSet()
			if err != nil {
				return err
			}
			err = db.ApplyChangeSet(cs)
			if err != nil {
				return err
			}
		} else if cl.Type() == raftlog.TypePreSplit {
			// PreSplit is handled by engine.
		} else {
			applier.execCustomLog(h.ctx, cl)
			wb := h.ctx.wb.getEngineWriteBatch(shard.ID)
			err := db.RecoverWrite(wb)
			if err != nil {
				return err
			}
			wb.Reset()
		}
	}
	newState := fromApplyState
	newState.appliedIndex = highIdx
	shard.RecoverSetProperty(applyStateKey, newState.Marshal())
	return nil
}

func (h *RecoverHandler) loadRegionMeta(id, ver uint64) (region *metapb.Region, committedIdx uint64, err error) {
	regionKey := RegionStateKeyByIDEpoch(id, &metapb.RegionEpoch{Version: ver, ConfVer: math.MaxUint64})
	err = h.raftDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Seek(regionKey)
		y.Assert(it.Valid())
		item := it.Item()
		val, _ := item.Value()
		state := new(raft_serverpb.RegionLocalState)
		err1 := state.Unmarshal(val)
		if err1 != nil {
			return err1
		}
		region = state.Region
		raftKey := RaftStateKey(region)
		item, err1 = txn.Get(raftKey)
		if err1 != nil {
			return err1
		}
		val, _ = item.Value()
		var raftState raftState
		raftState.Unmarshal(val)
		committedIdx = raftState.commit
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return region, committedIdx, nil
}

func (h *RecoverHandler) executeAdminRequest(a *applier, cmdReq *raft_cmdpb.RaftCmdRequest) error {
	adminReq := cmdReq.AdminRequest
	raftWB := new(RaftWriteBatch)
	if adminReq.Splits != nil {
		_, newRegions, err := a.splitGenNewRegionMetas(adminReq.Splits)
		if err != nil {
			return err
		}
		for _, region := range newRegions {
			WritePeerState(raftWB, region, raft_serverpb.PeerState_Normal, nil)
		}
	} else if adminReq.ChangePeer != nil {
		resp, _, err := a.execChangePeer(adminReq)
		if err != nil {
			return err
		}
		state := raft_serverpb.PeerState_Normal
		if a.pendingRemove {
			state = raft_serverpb.PeerState_Tombstone
		}
		WritePeerState(raftWB, resp.ChangePeer.Region, state, nil)
	}
	return raftWB.WriteToRaft(h.raftDB)
}
