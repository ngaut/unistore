package raftstore

import (
	"bytes"
	"encoding/binary"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/ngaut/unistore/tikv/raftstore/raftlog"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/table/memtable"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/log"
	"github.com/zhangjinpeng1987/raft"
	"go.uber.org/zap"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"
)

// MetaChangeListener implements the badger.MetaChangeListener interface.
type MetaChangeListener struct {
	mu    sync.Mutex
	msgCh chan<- Msg
	queue []Msg
}

func NewMetaChangeListener() *MetaChangeListener {
	return &MetaChangeListener{}
}

// OnChange implements the badger.MetaChangeListener interface.
func (l *MetaChangeListener) OnChange(e *protos.MetaChangeEvent) {
	msg := Msg{
		Type: MsgTypeGenerateEngineMetaChange,
		Data: e,
	}
	l.mu.Lock()
	ch := l.msgCh
	if ch == nil {
		l.queue = append(l.queue, msg)
	}
	l.mu.Unlock()
	if ch != nil {
		ch <- msg
	}
}

func (l *MetaChangeListener) initMsgCh(msgCh chan<- Msg) {
	l.mu.Lock()
	l.msgCh = msgCh
	queue := l.queue
	l.mu.Unlock()
	for _, msg := range queue {
		msgCh <- msg
	}
}

// MetaManager contains file metas in this store, it not only contains meta stored in local engine, but also
// the follower region's file meta synced from its leader.
// The follower region's file meta may not loaded from S3, so it can not be found in local engine.
type MetaManager struct {
	regionMetas sync.Map
	engine      *badger.ShardingDB
	raftEngine  *badger.DB
	metaLog     *os.File
	listener    *MetaChangeListener
	path        string
}

func NewEngineMetaManager(
	engine *badger.ShardingDB, raftEngine *badger.DB, path string, listener *MetaChangeListener) (*MetaManager, error) {
	m := &MetaManager{
		engine:     engine,
		raftEngine: raftEngine,
		path:       path,
		listener:   listener,
	}
	if err := m.loadRegions(); err != nil {
		return nil, err
	}
	if err := m.initMeta(); err != nil {
		return nil, err
	}
	return m, nil
}

func (mm *MetaManager) loadRegions() error {
	regionStateStartKey := RegionStateKey(0)
	regionStateEndKey := RegionStateKey(math.MaxUint64)
	snap := mm.engine.NewSnapshot(regionStateStartKey, regionStateEndKey)
	defer snap.Discard()
	iter := snap.NewIterator(mvcc.RaftCF, false, false)
	defer iter.Close()
	for iter.Seek(regionStateStartKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), regionStateEndKey) >= 0 {
			break
		}
		val, _ := item.Value()
		rs := new(rspb.RegionLocalState)
		err := rs.Unmarshal(val)
		if err != nil {
			return err
		}
		regionMeta := newRegionFileMeta(rs.Region, mm.engine.NumCFs(), badger.ShardMaxLevel)
		mm.regionMetas.Store(regionMeta.id, regionMeta)
	}
	return nil
}

func (mm *MetaManager) initMeta() error {
	metaLogPath := filepath.Join(mm.path, "meta-log")
	metaLog, err := y.OpenSyncedFile(metaLogPath, false)
	if err != nil {
		return err
	}
	stat, err := metaLog.Stat()
	if err != nil {
		return err
	}
	mm.metaLog = metaLog
	if stat.Size() > 0 {
		headerBin := make([]byte, 5)
		changeLogBin := make([]byte, 256*1024)
		for {
			_, err := io.ReadFull(metaLog, headerBin)
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}
			tp := headerBin[0]
			length := binary.LittleEndian.Uint32(headerBin[1:])
			if cap(changeLogBin) < int(length) {
				changeLogBin = make([]byte, length)
			} else {
				changeLogBin = changeLogBin[:length]
			}
			_, err = io.ReadFull(metaLog, changeLogBin)
			if err != nil {
				log.Error("failed to read meta log", zap.Error(err))
				return err
			}
			if tp == metaLogTypeLoadState {
				loaded := changeLogBin[0]
				regionID := binary.LittleEndian.Uint64(changeLogBin[1:])
				meta := mm.mustLoad(regionID)
				if loaded == 1 {
					meta.state = unsafe.Pointer(&loadingState{ok: true})
				} else {
					meta.state = unsafe.Pointer(&loadingState{})
				}
			} else {
				rlog := raftlog.NewEngineMetaChange(changeLogBin)
				mm.updateMeta(rlog)
			}
		}
	}
	return nil
}

var initialEndKey = []byte{255, 255, 255, 255, 255, 255, 255, 255}

func (mm *MetaManager) prepareBootstrap(initRegion *metapb.Region) error {
	peer := initRegion.Peers[0]
	header := raftlog.CustomHeader{
		RegionID: initRegion.Id,
		Epoch:    raftlog.NewEpoch(initRegion.RegionEpoch.Version, initRegion.RegionEpoch.ConfVer),
		PeerID:   peer.Id,
		StoreID:  peer.StoreId,
	}
	e := &protos.MetaChangeEvent{EndKey: initialEndKey}
	rlog := raftlog.BuildEngineMetaChangeLog(header, e)
	err := mm.writeMetaLogMetaChange(rlog.(*raftlog.EngineMetaChangeRaftLog))
	if err != nil {
		return err
	}
	regionMeta := newRegionFileMeta(initRegion, mm.engine.NumCFs(), badger.ShardMaxLevel)
	regionMeta.getLoadingState().ok = true
	mm.regionMetas.Store(initRegion.Id, regionMeta)
	return nil
}

func (mm *MetaManager) clearPrepareBootstrap(regionID uint64) {
	mm.regionMetas.Delete(regionID)
	mm.metaLog.Truncate(0)
	mm.metaLog.Seek(0, 0)
}

func (mm *MetaManager) getSnapshotData(regionID uint64) (*eraftpb.Snapshot, error) {
	rval, ok := mm.regionMetas.Load(regionID)
	if !ok {
		return nil, errors.New("getSnapshotData: region not found")
	}
	rm := rval.(*regionFileMeta)

	e := rm.convertToChangeEvent()
	var maxL0ID uint64
	for _, l0 := range e.AddedL0Files {
		if l0.ID > maxL0ID {
			maxL0ID = l0.ID
		}
	}
	regionKey := RegionStateKey(regionID)

	eSnap := mm.engine.NewSnapshot(regionKey, regionKey)
	regionLocalState, err := engineSnapshotGetRegionLocalState(eSnap, y.KeyWithTs(regionKey, 0))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	region := regionLocalState.Region
	rawStartKey := RawStartKey(region)
	rawEndKey := RawEndKey(region)
	applyStateKey := ShardingApplyStateKey(rawStartKey, region.Id)
	applyState, err := engineSnapshotGetApplyState(eSnap, y.KeyWithTs(applyStateKey, 0))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	deltaEntries := new(deltaEntries)
	for cf := 0; cf < mm.engine.NumCFs(); cf++ {
		iter := eSnap.NewDeltaIterator(cf, maxL0ID)
		for iter.Seek(rawStartKey); iter.Valid(); iter.Next() {
			item := iter.Item()
			if bytes.Compare(item.Key(), rawEndKey) >= 0 {
				break
			}
			val, _ := item.Value()
			e := deltaEntry{
				cf:       cf,
				key:      item.Key(),
				val:      val,
				userMeta: item.UserMeta(),
				version:  item.Version(),
			}
			deltaEntries.encodeEntry(e)
		}
		iter.Close()
	}
	snapData := &snapData{
		region:       region,
		applyState:   applyState,
		changeEvent:  e,
		deltaEntries: *deltaEntries,
		maxReadTS:    eSnap.GetReadTS(),
	}
	snap := &eraftpb.Snapshot{
		Metadata: &eraftpb.SnapshotMetadata{},
		Data:     snapData.Marshal(),
	}
	snap.Metadata.Index = applyState.appliedIndex
	snap.Metadata.Term = applyState.appliedIndexTerm
	confState := confStateFromRegion(region)
	snap.Metadata.ConfState = &confState
	return snap, nil
}

type snapData struct {
	region       *metapb.Region
	applyState   *applyState
	changeEvent  *protos.MetaChangeEvent
	deltaEntries deltaEntries
	maxReadTS    uint64
}

type deltaEntry struct {
	cf       int
	key      []byte
	val      []byte
	userMeta []byte
	version  uint64
}

type deltaEntries struct {
	data []byte
}

func (d *deltaEntries) encodeEntry(e deltaEntry) {
	d.data = append(d.data, byte(e.cf))
	d.data = appendSlice(d.data, e.key)
	d.data = appendSlice(d.data, e.val)
	d.data = appendSlice(d.data, e.userMeta)
	d.data = appendU64(d.data, e.version)
}

func (d *deltaEntries) decodeEntry() deltaEntry {
	var e deltaEntry
	e.cf = int(d.data[0])
	d.data = d.data[1:]
	e.key, d.data = cutSlices(d.data)
	e.val, d.data = cutSlices(d.data)
	e.userMeta, d.data = cutSlices(d.data)
	e.version = binary.LittleEndian.Uint64(d.data)
	d.data = d.data[8:]
	return e
}

func (sd *snapData) Marshal() []byte {
	regionData, _ := sd.region.Marshal()
	applyStateData := sd.applyState.Marshal()
	changeData, _ := sd.changeEvent.Marshal()
	buf := make([]byte, 0, 4+len(regionData)+4+len(applyStateData)+4+len(changeData)+4+len(sd.deltaEntries.data))
	buf = appendSlice(buf, regionData)
	buf = appendSlice(buf, applyStateData)
	buf = appendSlice(buf, changeData)
	buf = appendSlice(buf, sd.deltaEntries.data)
	buf = appendU64(buf, sd.maxReadTS)
	return buf
}

func (sd *snapData) Unmarshal(data []byte) error {
	sd.region = new(metapb.Region)
	element, data := cutSlices(data)
	err := sd.region.Unmarshal(element)
	if err != nil {
		return err
	}
	sd.applyState = new(applyState)
	element, data = cutSlices(data)
	sd.applyState.Unmarshal(element)
	sd.changeEvent = new(protos.MetaChangeEvent)
	element, data = cutSlices(data)
	err = sd.changeEvent.Unmarshal(element)
	if err != nil {
		return err
	}
	sd.deltaEntries.data, data = cutSlices(data)
	sd.maxReadTS = binary.LittleEndian.Uint64(data)
	return nil
}

func appendSlice(buf []byte, element []byte) []byte {
	buf = append(buf, make([]byte, 4)...)
	binary.LittleEndian.PutUint32(buf[len(buf)-4:], uint32(len(element)))
	return append(buf, element...)
}

func appendU64(buf []byte, v uint64) []byte {
	tmp := make([]byte, 8)
	binary.LittleEndian.PutUint64(tmp, v)
	return append(buf, tmp...)
}

func cutSlices(data []byte) (element []byte, remain []byte) {
	length := binary.LittleEndian.Uint32(data)
	data = data[4:]
	return data[:length], data[length:]
}

func engineSnapshotGetRegionLocalState(eSnap *badger.Snapshot, key y.Key) (*rspb.RegionLocalState, error) {
	item, err := eSnap.Get(mvcc.RaftCF, key)
	if err != nil {
		return nil, err
	}
	val, err := item.Value()
	if err != nil {
		return nil, err
	}
	regionLocalState := new(rspb.RegionLocalState)
	err = regionLocalState.Unmarshal(val)
	if err != nil {
		return nil, err
	}
	return regionLocalState, nil
}

func engineSnapshotGetApplyState(eSnap *badger.Snapshot, key y.Key) (*applyState, error) {
	item, err := eSnap.Get(mvcc.RaftCF, key)
	if err != nil {
		return nil, err
	}
	val, err := item.Value()
	if err != nil {
		return nil, err
	}
	applyState := &applyState{}
	applyState.Unmarshal(val)
	return applyState, nil
}

const (
	metaLogTypeMetaChange byte = 0
	metaLogTypeLoadState  byte = 1
)

func (mm *MetaManager) writeMetaLogMetaChange(l *raftlog.EngineMetaChangeRaftLog) error {
	// TODO: add checksum.
	data := l.Marshal()
	headerBin := make([]byte, 5)
	headerBin[0] = metaLogTypeMetaChange
	binary.LittleEndian.PutUint32(headerBin[1:], uint32(len(data)))
	_, err := mm.metaLog.Write(headerBin)
	if err != nil {
		return err
	}
	_, err = mm.metaLog.Write(data)
	if err != nil {
		return err
	}
	return mm.metaLog.Sync()
}

func (mm *MetaManager) writeMetaLogLoadState(regionID uint64, loaded bool) error {
	data := make([]byte, 1+4+8+1)
	data[0] = metaLogTypeLoadState
	binary.LittleEndian.PutUint32(data[1:], 9)
	if loaded {
		data[5] = 1
	} else {
		data[5] = 0
	}
	binary.LittleEndian.PutUint64(data[len(data)-8:], regionID)
	_, err := mm.metaLog.Write(data)
	if err != nil {
		return err
	}
	return mm.metaLog.Sync()
}

func (mm *MetaManager) applyMetaChange(rlog *raftlog.EngineMetaChangeRaftLog) error {
	err := mm.writeMetaLogMetaChange(rlog)
	if err != nil {
		return err
	}
	mm.updateMeta(rlog)
	return nil
}

func (mm *MetaManager) updateMeta(rlog *raftlog.EngineMetaChangeRaftLog) {
	e := rlog.UnmarshalEvent()
	regionID := rlog.RegionID()
	val, ok := mm.regionMetas.Load(regionID)
	y.Assert(ok)
	regionMeta := val.(*regionFileMeta)
	if len(e.AddedL0Files) == 1 && len(e.AddedFiles) == 0 {
		// Flush event, clear all the unneeded pending raft logs.
		commitTS := e.AddedL0Files[0].CommitTS
		snap := mm.engine.NewSnapshot(regionMeta.startKey, regionMeta.startKey)
		item, err := snap.Get(mvcc.RaftCF, y.KeyWithTs(ShardingApplyStateKey(regionMeta.startKey, regionID), commitTS))
		if err == nil {
			v, _ := item.Value()
			var applyState applyState
			applyState.Unmarshal(v)
			for i := 0; i < len(regionMeta.pendingIdx); i++ {
				if regionMeta.pendingIdx[i] > applyState.appliedIndex {
					regionMeta.pending = regionMeta.pending[i:]
					regionMeta.pendingIdx = regionMeta.pendingIdx[i:]
					break
				}
			}
		} else {
			log.S().Infof("failed to get with commitTS %d, raft log len %d", commitTS, len(regionMeta.pending))
			latest, err1 := snap.Get(mvcc.RaftCF, y.KeyWithTs(ShardingApplyStateKey(regionMeta.startKey, regionID), commitTS))
			if err1 != nil {
				log.S().Info("get with latest ts", err1)
			} else {
				v, _ := latest.Value()
				var applyState applyState
				applyState.Unmarshal(v)
				log.S().Infof("get latest apply index %d", applyState.appliedIndex)
			}
		}

	}
	regionMeta.mu.Lock()
	defer regionMeta.mu.Unlock()
	for _, l0Meta := range e.AddedL0Files {
		regionMeta.addL0(l0Meta)
	}
	for _, id := range e.RemovedL0Files {
		regionMeta.removeL0(id)
	}
	for _, meta := range e.RemovedFiles {
		if meta.Level == 0 {
			regionMeta.removeL0(meta.ID)
		} else {
			regionMeta.removeFile(meta)
		}
	}
	for _, meta := range e.AddedFiles {
		regionMeta.addFile(meta)
	}
}

func (mm *MetaManager) applySnapshot(snap *snapData) {
	regionMeta := newRegionFileMeta(snap.region, mm.engine.NumCFs(), badger.ShardMaxLevel)
	regionMeta.snapTS = snap.maxReadTS
	delta := snap.deltaEntries
	arenaSize := len(delta.data)*3/2 + 1024 // Make sure the arena is large enough.
	cfTable := memtable.NewCFTable(int64(arenaSize), mm.engine.NumCFs())
	for len(delta.data) > 0 {
		entry := delta.decodeEntry()
		v := y.ValueStruct{Value: entry.val, UserMeta: entry.userMeta, Version: entry.version}
		cfTable.Put(entry.cf, entry.key, v)
	}
	regionMeta.memTbls = append(regionMeta.memTbls, cfTable)
	for _, l0Meta := range snap.changeEvent.AddedL0Files {
		regionMeta.addL0(l0Meta)
	}
	for _, fileMeta := range snap.changeEvent.AddedFiles {
		regionMeta.addFile(fileMeta)
	}
	mm.regionMetas.Store(regionMeta.id, regionMeta)
}

func (mm *MetaManager) split(derived *metapb.Region, regions []*metapb.Region) {
	oldRegionVal, ok := mm.regionMetas.Load(derived.Id)
	y.Assert(ok)
	oldRegionFileMeta := oldRegionVal.(*regionFileMeta)
	oldState := oldRegionFileMeta.getLoadingState()
	newRegionFileMetas := make([]*regionFileMeta, 0, len(regions))
	for _, region := range regions {
		regionFileMeta := newRegionFileMeta(region, mm.engine.NumCFs(), badger.ShardMaxLevel)
		if oldState.ok {
			regionFileMeta.getLoadingState().ok = true
		}
		newRegionFileMetas = append(newRegionFileMetas, regionFileMeta)
	}
	sort.Slice(newRegionFileMetas, func(i, j int) bool {
		return bytes.Compare(newRegionFileMetas[i].startKey, newRegionFileMetas[j].startKey) < 0
	})
	// Redistribute the old files to new regions.
	// The files have been split already by the region boundary.
	for _, l0 := range oldRegionFileMeta.l0s {
		smallest := l0.MultiCFSmallest[0]
		idx := sort.Search(len(newRegionFileMetas), func(i int) bool {
			return bytes.Compare(smallest, newRegionFileMetas[i].endKey) < 0
		})
		newRegionMeta := newRegionFileMetas[idx]
		newRegionMeta.l0s = append(newRegionMeta.l0s, l0)
	}
	for cf := 0; cf < len(oldRegionFileMeta.cfs); cf++ {
		for lvl := 1; lvl < badger.ShardMaxLevel; lvl++ {
			levelMeta := &oldRegionFileMeta.cfs[cf].levels[lvl-1]
			for _, fileMeta := range levelMeta.files {
				idx := sort.Search(len(newRegionFileMetas), func(i int) bool {
					return bytes.Compare(fileMeta.Smallest, newRegionFileMetas[i].endKey) < 0
				})
				newRegionMeta := newRegionFileMetas[idx]
				newLevelMeta := &newRegionMeta.cfs[cf].levels[lvl-1]
				newLevelMeta.files = append(newLevelMeta.files, fileMeta)
			}
		}
	}
	for _, newRegionMeta := range newRegionFileMetas {
		mm.regionMetas.Store(newRegionMeta.id, newRegionMeta)
	}
}

func (mm *MetaManager) followerApply(regionID, index uint64, rlog raftlog.RaftLog) {
	meta := mm.mustLoad(regionID)
	meta.pending = append(meta.pending, rlog)
	meta.pendingIdx = append(meta.pendingIdx, index)
}

func (mm *MetaManager) ingestRegionFiles(regionID uint64) {
	meta := mm.mustLoad(regionID)
	state := (*loadingState)(atomic.LoadPointer(&meta.state))
	if state.ok {
		return
	}
	if state.wg != nil {
		// Already loading.
		return
	}
	newState := &loadingState{wg: new(sync.WaitGroup)}
	newState.wg.Add(1)
	if atomic.CompareAndSwapPointer(&meta.state, unsafe.Pointer(state), unsafe.Pointer(newState)) {
		tree := &badger.IngestTree{MaxTS: meta.snapTS, Meta: meta.convertToChangeEvent(), Delta: append([]*memtable.CFTable{}, meta.memTbls...)}
		go mm.loadFilesAndApplyToLatestLog(meta, tree, newState.wg)
	}
}

func (mm *MetaManager) loadFilesAndApplyToLatestLog(meta *regionFileMeta, tree *badger.IngestTree, wg *sync.WaitGroup) {
	var err error
	defer func() {
		finishState := &loadingState{ok: err == nil, err: err}
		atomic.SwapPointer(&meta.state, unsafe.Pointer(finishState))
		wg.Done()
	}()
	err = mm.engine.Ingest(tree)
	if err != nil {
		return
	}
	e := tree.Meta
	snap := mm.engine.NewSnapshot(e.StartKey, e.EndKey)
	defer snap.Discard()
	buffer := memtable.NewCFTable(64*1024*1024, mm.engine.NumCFs())
	snap.SetBuffer(buffer)
	aCtx := &applyContext{dbSnap: snap, wb: new(WriteBatch)}
	applier := &applier{}
	for i := 0; i < len(meta.pending); i++ {
		applier.execWriteCmd(aCtx, meta.pending[i])
		aCtx.wb.WriteToBuffer(buffer, snap.GetReadTS())
		aCtx.wb.Reset()
	}
	mm.engine.IngestBuffer(tree.Meta.StartKey, buffer)
}

func (mm *MetaManager) mustLoad(regionID uint64) *regionFileMeta {
	val, ok := mm.regionMetas.Load(regionID)
	y.Assert(ok)
	return val.(*regionFileMeta)
}

func (mm *MetaManager) onRoleChange(regionID uint64, raftState raft.StateType) {
	if raftState == raft.StateLeader {
		mm.ingestRegionFiles(regionID)
	}
}

func (mm *MetaManager) isReady(regionID uint64) (ok bool, wg *sync.WaitGroup) {
	meta := mm.mustLoad(regionID)
	state := (*loadingState)(atomic.LoadPointer(&meta.state))
	return state.ok, state.wg
}

// regionFileMeta organize file metas in LSM tree structure for easier update.
type regionFileMeta struct {
	id         uint64
	mu         sync.Mutex
	state      unsafe.Pointer
	pending    []raftlog.RaftLog
	pendingIdx []uint64

	snapTS   uint64
	startKey []byte
	endKey   []byte
	memTbls  []*memtable.CFTable
	l0s      []*protos.L0FileMeta
	cfs      []cfFileMeta
}

func newRegionFileMeta(region *metapb.Region, numCFs, maxLevel int) *regionFileMeta {
	rfm := &regionFileMeta{id: region.Id, startKey: RawStartKey(region), endKey: RawEndKey(region)}
	rfm.state = unsafe.Pointer(new(loadingState))
	rfm.cfs = make([]cfFileMeta, numCFs)
	for cf := 0; cf < numCFs; cf++ {
		rfm.cfs[cf] = cfFileMeta{levels: make([]levelFileMeta, maxLevel-1)}
	}
	return rfm
}

func (rfm *regionFileMeta) removeL0(id uint64) {
	var i int
	for ; i < len(rfm.l0s); i++ {
		if rfm.l0s[i].ID == id {
			break
		}
	}
	if i < len(rfm.l0s) {
		rfm.l0s = append(rfm.l0s[i:], rfm.l0s[:i+1]...)
	}
}

func (rfm *regionFileMeta) getLoadingState() *loadingState {
	return (*loadingState)(atomic.LoadPointer(&rfm.state))
}

func (rfm *regionFileMeta) addL0(meta *protos.L0FileMeta) {
	for i := 0; i < len(rfm.l0s); i++ {
		if rfm.l0s[i].ID == meta.ID {
			// The meta is already added.
			return
		}
	}
	rfm.l0s = append(rfm.l0s, meta)
}

func (rfm *regionFileMeta) removeFile(meta *protos.FileMeta) {
	level := &rfm.cfs[meta.CF].levels[meta.Level-1]
	var i int
	for ; i < len(level.files); i++ {
		if level.files[i].ID == meta.ID {
			break
		}
	}
	if i < len(level.files) {
		level.files = append(level.files[i:], level.files[i+1:]...)
	}
}

func (rfm *regionFileMeta) addFile(meta *protos.FileMeta) {
	level := &rfm.cfs[meta.CF].levels[meta.Level-1]
	for i := 0; i < len(level.files); i++ {
		if level.files[i].ID == meta.ID {
			// The meta is already added.
			return
		}
	}
	level.files = append(level.files, meta)
}

func (rfm *regionFileMeta) convertToChangeEvent() *protos.MetaChangeEvent {
	rfm.mu.Lock()
	l0Cnt := len(rfm.l0s)
	var lnCnt int
	for _, cf := range rfm.cfs {
		for _, lvl := range cf.levels {
			lnCnt += len(lvl.files)
		}
	}
	// Avoid allocate memory in lock.
	rfm.mu.Unlock()
	// TODO: MetaChangeEvent is not the best pb to represent snapshot we just use it for convenience.
	// Only AddedL0s and AddedFiles fields are used.
	e := &protos.MetaChangeEvent{
		AddedL0Files: make([]*protos.L0FileMeta, 0, l0Cnt),
		AddedFiles:   make([]*protos.FileMeta, 0, lnCnt),
	}
	rfm.mu.Lock()
	e.StartKey = rfm.startKey
	e.EndKey = rfm.endKey
	e.AddedL0Files = append(e.AddedL0Files, rfm.l0s...)
	for _, cf := range rfm.cfs {
		for _, lvl := range cf.levels {
			e.AddedFiles = append(e.AddedFiles, lvl.files...)
		}
	}
	rfm.mu.Unlock()
	return e
}

type cfFileMeta struct {
	levels []levelFileMeta
}

type levelFileMeta struct {
	files []*protos.FileMeta
}

type loadingState struct {
	ok  bool
	wg  *sync.WaitGroup
	err error
}
