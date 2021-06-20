package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ngaut/unistore/enginepb"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type shardTester struct {
	shardIDAlloc uint64
	writeCh      chan interface{}
	db           *Engine
	shardTree    unsafe.Pointer
	wg           sync.WaitGroup
	writeLogs    *writeLogStore
}

func newShardTester(db *Engine) *shardTester {
	tester := &shardTester{
		shardIDAlloc: 0,
		writeCh:      make(chan interface{}, 256),
		db:           db,
		writeLogs:    &writeLogStore{m: map[shardIDVer]*testerWriteLog{}},
	}
	var shards []*Shard
	db.shardMap.Range(func(key, value interface{}) bool {
		shard := value.(*Shard)
		shards = append(shards, shard)
		return true
	})
	sort.Slice(shards, func(i, j int) bool {
		return bytes.Compare(shards[i].Start, shards[j].Start) < 0
	})
	tester.shardTree = unsafe.Pointer(&testerShardTree{
		shards: shards,
	})
	tester.wg.Add(1)
	go tester.runWriter()
	return tester
}

type testerWriteRequest struct {
	shard   *Shard
	entries []*testerEntry
	replay  bool
	resp    chan error
}

type testerEntry struct {
	cf  int
	key []byte
	val []byte
	ver uint64
}

type testerShardTree struct {
	shards []*Shard
}

type testerWriteLog struct {
	beginIdx int
	entries  [][]*testerEntry
}

type shardIDVer struct {
	id  uint64
	ver uint64
}

func newIDVer(shard *Shard) shardIDVer {
	return shardIDVer{id: shard.ID, ver: shard.Ver}
}

type writeLogStore struct {
	m map[shardIDVer]*testerWriteLog
}

func (wls *writeLogStore) append(shard *Shard, entries []*testerEntry) {
	idver := newIDVer(shard)
	wl, ok := wls.m[idver]
	if !ok {
		wl = &testerWriteLog{}
		wls.m[idver] = wl
	}
	wl.entries = append(wl.entries, entries)
}

func (wls *writeLogStore) getEntries(shard *Shard, idx int) []*testerEntry {
	idver := newIDVer(shard)
	wl, ok := wls.m[idver]
	if !ok {
		return nil
	}
	return wl.entries[idx-wl.beginIdx]
}

func (wls *writeLogStore) latestIndex(shard *Shard) int {
	wl, ok := wls.m[newIDVer(shard)]
	if !ok {
		return -1
	}
	return wl.beginIdx + len(wl.entries) - 1
}

func (tree *testerShardTree) getShard(key []byte) *Shard {
	for _, shd := range tree.shards {
		if bytes.Compare(key, shd.End) >= 0 {
			continue
		}
		if bytes.Compare(shd.Start, key) <= 0 {
			return shd
		}
		break
	}
	return nil
}

func (tree *testerShardTree) buildGetSnapRequests(start, end []byte) []*testerGetSnapRequest {
	var results []*testerGetSnapRequest
	for _, shd := range tree.shards {
		if shd.OverlapRange(start, end) {
			reqStart := shd.Start
			if bytes.Compare(shd.Start, start) < 0 {
				reqStart = start
			}
			reqEnd := shd.End
			if bytes.Compare(shd.End, end) > 0 {
				reqEnd = end
			}
			req := &testerGetSnapRequest{
				start: reqStart,
				end:   reqEnd,
			}
			results = append(results, req)
		}
	}
	return results
}

func (st *shardTester) split(oldID uint64, newShards []*Shard) {
	newTree := &testerShardTree{}
	tree := st.loadShardTree()
	for _, shd := range tree.shards {
		if shd.ID == oldID {
			for _, newShd := range newShards {
				newTree.shards = append(newTree.shards, newShd)
				if newShd.ID == oldID {
					st.writeLogs.m[newIDVer(newShd)] = &testerWriteLog{beginIdx: st.writeLogs.latestIndex(shd)}
				}
			}
		} else {
			newTree.shards = append(newTree.shards, shd)
		}
	}
	atomic.StorePointer(&st.shardTree, unsafe.Pointer(newTree))
}

type testerPreSplitRequest struct {
	shardID uint64
	ver     uint64
	keys    [][]byte
	resp    chan error
}

type testerFinishSplitRequest struct {
	shardID  uint64
	ver      uint64
	newProps []*enginepb.Properties
	keys     [][]byte
	resp     chan error
}

func (st *shardTester) runWriter() {
	defer st.wg.Done()
	for {
		val := <-st.writeCh
		switch x := val.(type) {
		case nil:
			return
		case *testerWriteRequest:
			st.handleWriteRequest(x)
		case *testerPreSplitRequest:
			err := st.db.PreSplit(x.shardID, x.ver, x.keys)
			if err != nil {
				x.resp <- err
			} else {
				changeSet, err1 := st.db.SplitShardFiles(x.shardID, x.ver)
				if err1 != nil {
					x.resp <- err1
				} else {
					x.resp <- st.db.ApplyChangeSet(changeSet)
				}
			}
		case *testerFinishSplitRequest:
			newShards, err := st.db.FinishSplit(x.shardID, x.ver, x.newProps)
			if err == nil {
				for _, nShard := range newShards {
					st.db.TriggerFlush(nShard, 0)
				}
				log.S().Info("tester finish split")
				st.split(x.shardID, newShards)
			}
			x.resp <- err
		case *testerGetSnapRequest:
			st.handleIterateRequest(x)
		}
	}
}

func (st *shardTester) handleWriteRequest(req *testerWriteRequest) {
	shard := st.db.GetShard(req.shard.ID)
	if shard.Ver != req.shard.Ver {
		req.resp <- errShardNotMatch
		return
	}
	if !req.replay {
		st.writeLogs.append(shard, req.entries)
	}
	wb := st.db.NewWriteBatch(shard)
	for _, e := range req.entries {
		err := wb.Put(e.cf, e.key, y.ValueStruct{Value: e.val, Version: e.ver})
		if err != nil {
			req.resp <- err
			return
		}
	}
	idxBin := make([]byte, 4)
	binary.LittleEndian.PutUint32(idxBin, uint32(st.writeLogs.latestIndex(req.shard)))
	wb.SetProperty(appliedIndex, idxBin)
	st.db.Write(wb)
	req.resp <- nil
}

const appliedIndex = "applied_index"

func (st *shardTester) handleIterateRequest(req *testerGetSnapRequest) {
	shard := st.db.GetShard(req.shard.ID)
	if shard == nil {
		log.Error("shard not found", zap.Uint64("shard", req.shard.ID))
		req.resp <- errShardNotFound
		return
	}
	if shard.Ver != req.shard.Ver {
		log.S().Infof("shard version not match, %d %d", shard.Ver, req.shard.Ver)
		req.resp <- errShardNotMatch
		return
	}
	req.snap = st.db.NewSnapAccess(shard)
	req.resp <- nil
}

func (st *shardTester) loadShardTree() *testerShardTree {
	return (*testerShardTree)(atomic.LoadPointer(&st.shardTree))
}

func (st *shardTester) write(entries ...*testerEntry) error {
	tree := st.loadShardTree()
	requests := make(map[uint64]*testerWriteRequest)
	for _, entry := range entries {
		shard := tree.getShard(entry.key)
		if shard == nil {
			for _, treeShard := range tree.shards {
				log.S().Infof("tree shard %s %s", treeShard.Start, treeShard.End)
			}
			return fmt.Errorf("shard not found for key %s", entry.key)
		}
		shardID := shard.ID
		req, ok := requests[shardID]
		if !ok {
			req = &testerWriteRequest{
				shard: shard,
				resp:  make(chan error, 1),
			}
			requests[shardID] = req
		}
		req.entries = append(req.entries, entry)
	}
	for _, req := range requests {
		st.writeCh <- req
	}
	var retries []*testerEntry
	for _, req := range requests {
		err := <-req.resp
		if err == errShardNotMatch {
			log.S().Infof("write shard not match %s %s %d %d", entries[0].key, entries[len(entries)-1].key, req.shard.ID, req.shard.Ver)
			retries = append(retries, req.entries...)
		} else if err != nil {
			return err
		}
	}
	if len(retries) != 0 {
		time.Sleep(time.Millisecond * 10)
		err := st.write(retries...)
		log.S().Infof("retry %d entries err %v", len(retries), err)
	}
	return nil
}

func (st *shardTester) preSplit(shardID, ver uint64, keys [][]byte) error {
	req := &testerPreSplitRequest{
		shardID: shardID,
		ver:     ver,
		keys:    keys,
		resp:    make(chan error, 1),
	}
	st.writeCh <- req
	return <-req.resp
}

func (st *shardTester) finishSplit(shardID, ver uint64, props []*enginepb.Properties) error {
	req := &testerFinishSplitRequest{
		shardID:  shardID,
		ver:      ver,
		newProps: props,
		resp:     make(chan error, 1),
	}
	st.writeCh <- req
	return <-req.resp
}

type testerGetSnapRequest struct {
	shard *Shard
	start []byte
	end   []byte
	resp  chan error
	snap  *SnapAccess
}

func (st *shardTester) iterate(start, end []byte, cf int, iterFunc func(key, val []byte)) error {
	tree := st.loadShardTree()
	var requests []*testerGetSnapRequest
	for _, shd := range tree.shards {
		if shd.OverlapRange(start, end) {
			reqStart := shd.Start
			if bytes.Compare(shd.Start, start) < 0 {
				reqStart = start
			}
			reqEnd := shd.End
			if bytes.Compare(shd.End, end) > 0 {
				reqEnd = end
			}
			req := &testerGetSnapRequest{
				shard: shd,
				start: reqStart,
				end:   reqEnd,
				resp:  make(chan error, 1),
			}
			requests = append(requests, req)
		}
	}
	for _, req := range requests {
		st.writeCh <- req
	}
	for _, req := range requests {
		err := <-req.resp
		if err == errShardNotMatch {
			time.Sleep(time.Millisecond * 10)
			err = st.iterate(req.start, req.end, cf, iterFunc)
			if err != nil {
				return err
			}
		} else if err != nil {
			log.Error("iterate req error", zap.Error(err))
			return err
		} else {
			snap := req.snap
			iter := snap.NewIterator(cf, false, false)
			for iter.Seek(req.start); iter.Valid(); iter.Next() {
				item := iter.Item()
				if bytes.Compare(item.Key(), req.end) >= 0 {
					break
				}
				iterFunc(item.Key(), item.val)
			}
			iter.Close()
			snap.Discard()
		}
	}
	return nil
}

func (st *shardTester) close() {
	st.writeCh <- nil
	st.wg.Wait()
}

func (st *shardTester) Recover(db *Engine, shard *Shard, meta *ShardMeta, toState *enginepb.Properties) error {
	val, ok := shard.RecoverGetProperty(appliedIndex)
	if !ok {
		return errors.New("no applied index")
	}
	start := int(binary.LittleEndian.Uint32(val)) + 1
	end := st.writeLogs.latestIndex(shard) + 1
	if toState != nil {
		val, ok = GetShardProperty(appliedIndex, toState)
		if !ok {
			return errors.New("no applied index")
		}
		end = int(binary.LittleEndian.Uint32(val)) + 1
	}
	for i := start; i < end; i++ {
		entries := st.writeLogs.getEntries(shard, i)
		wb := db.NewWriteBatch(shard)
		for _, e := range entries {
			err := wb.Put(e.cf, e.key, y.ValueStruct{Version: e.ver, Value: e.val})
			if err != nil {
				return err
			}
		}
		db.Write(wb)
	}
	return nil
}
