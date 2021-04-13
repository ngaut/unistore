package raftstore

import (
	"encoding/binary"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/kvproto/pkg/metapb"
	"sync"
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
func (l *MetaChangeListener) OnChange(e *protos.ShardChangeSet) {
	y.Assert(e.ShardID != 0)
	msg := NewPeerMsg(MsgTypeGenerateEngineChangeSet, e.ShardID, e)
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

/*
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
	regionStateStartKey := RegionMetaPrefixKey(0)
	regionStateEndKey := RegionMetaPrefixKey(math.MaxUint64)
	err := mm.raftEngine.View(func(txn *badger.Txn) error {
		itOpt := badger.DefaultIteratorOptions
		itOpt.Reverse = true
		it := txn.NewIterator(itOpt)
		defer it.Close()
		for it.Seek(regionStateEndKey); it.Valid(); it.Next() {
			item := it.Item()
			if bytes.Compare(item.Key(), regionStateStartKey) < 0 {
				break
			}
			val, _ := item.Value()
			rs := new(rspb.RegionLocalState)
			err := rs.Unmarshal(val)
			if err != nil {
				return err
			}
			regionMeta := newRegionFileMeta(rs.Region, mm.engine.NumCFs(), badger.ShardMaxLevel)
			if _, ok := mm.regionMetas.Load(regionMeta.id); ok {
				continue
			}
			mm.regionMetas.Store(regionMeta.id, regionMeta)
		}
		return nil
	})
	return err
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
	e := &protos.ShardChangeSet{
		ShardID: initRegion.Id,
		ShardVer: initRegion.RegionEpoch.Version,
		Snapshot: &protos.ShardSnapshot{
			End: initialEndKey,
			Properties: &protos.ShardProperties{
				ShardID: initRegion.Id,
				Keys:    []string{applyStateKey},
				Values:  [][]byte{newInitialApplyState().Marshal()},
			},
		},
	}
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
*/
type snapData struct {
	region    *metapb.Region
	changeSet *protos.ShardChangeSet
	maxReadTS uint64
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
	changeData, _ := sd.changeSet.Marshal()
	buf := make([]byte, 0, 4+len(regionData)+4+len(changeData))
	buf = appendSlice(buf, regionData)
	buf = appendSlice(buf, changeData)
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
	sd.changeSet = new(protos.ShardChangeSet)
	element, data = cutSlices(data)
	err = sd.changeSet.Unmarshal(element)
	if err != nil {
		return err
	}
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

/*

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
	regionMeta.mu.Lock()
	defer regionMeta.mu.Unlock()
	for _, id := range e. {
		regionMeta.removeFile(id)
	}
	for _, l0Meta := range e.L0Creates {
		regionMeta.addL0(l0Meta)
		for i, key := range l0Meta.Properties.Keys {
			if key == applyStateKey {
				val := l0Meta.Properties.Values[i]
				var applyState applyState
				applyState.Unmarshal(val)
				regionMeta.stableIdx = applyState.appliedIndex
			}
		}
	}
	for _, meta := range e.TableCreates {
		regionMeta.addFile(meta)
	}
}

func (mm *MetaManager) applySnapshot(snap *snapData) {
	regionMeta := newRegionFileMeta(snap.region, mm.engine.NumCFs(), badger.ShardMaxLevel)
	regionMeta.snapTS = snap.maxReadTS
	delta := snap.deltaEntries
	arenaSize := int64(len(delta.data)*3/2) // Make sure the arena is large enough.
	maxTblSize := mm.engine.GetOpt().MaxMemTableSize
	if arenaSize < maxTblSize {
		arenaSize = maxTblSize
	}
	cfTable := memtable.NewCFTable(arenaSize, mm.engine.NumCFs())
	for len(delta.data) > 0 {
		entry := delta.decodeEntry()
		v := y.ValueStruct{Value: entry.val, UserMeta: entry.userMeta, Version: entry.version}
		cfTable.Put(entry.cf, entry.key, v)
	}
	regionMeta.memTbls = append(regionMeta.memTbls, cfTable)
	for _, l0Meta := range snap.changeSet.L0Creates {
		regionMeta.addL0(l0Meta)
	}
	for _, fileMeta := range snap.changeSet.TableCreates {
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
		if idx == len(newRegionFileMetas) {
			log.S().Errorf("smallest %v, end key %v, old end key %v",
				smallest, newRegionFileMetas[len(newRegionFileMetas)-1].endKey, oldRegionFileMeta.endKey)
		}
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

func (mm *MetaManager) followerApply(regionID, index uint64) {
	meta := mm.mustLoad(regionID)
	meta.appliedIdx = index
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

func (mm *MetaManager) clearFiles(regionID uint64) {
	val, ok := mm.regionMetas.Load(regionID)
	if !ok {
		return
	}
	meta := val.(*regionFileMeta)
	go mm.engine.DeleteRange(meta.startKey, meta.endKey, false)
}

func (mm *MetaManager) loadFilesAndApplyToLatestLog(meta *regionFileMeta, tree *badger.IngestTree, wg *sync.WaitGroup) error {
	var err error
	defer func() {
		finishState := &loadingState{ok: err == nil, err: err}
		atomic.SwapPointer(&meta.state, unsafe.Pointer(finishState))
		wg.Done()
	}()
	log.Info("ingest region", zap.Uint64("id", meta.id))
	err = mm.engine.Ingest(tree)
	if err != nil {
		return err
	}
	entries, _, err := fetchEntriesTo(mm.raftEngine, meta.id, atomic.LoadUint64(&meta.stableIdx)+1, meta.appliedIdx, meta.appliedIdx, nil)
	if err != nil {
		return err
	}
	e := tree.Meta
	snap := mm.engine.NewSnapshot(e.StartKey, e.EndKey)
	defer snap.Discard()
	buffer := memtable.NewCFTable(64*1024*1024, mm.engine.NumCFs())
	snap.SetBuffer(buffer)
	aCtx := &applyContext{dbSnap: snap, wb: new(WriteBatch)}
	applier := &applier{}
	for i := 0; i < len(entries); i++ {
		entry := &entries[i]
		if entry.EntryType != eraftpb.EntryType_EntryNormal || len(entry.Data) == 0 {
			continue
		}
		rlog := raftlog.DecodeLog(entry)
		applier.execWriteCmd(aCtx, rlog)
		aCtx.wb.WriteToBuffer(buffer, snap.GetReadTS())
		aCtx.wb.Reset()
	}
	mm.engine.IngestBuffer(tree.Meta.StartKey, buffer)
	return nil
}

func (mm *MetaManager) mustLoad(regionID uint64) *regionFileMeta {
	val, ok := mm.regionMetas.Load(regionID)
	y.Assert(ok)
	return val.(*regionFileMeta)
}

func (mm *MetaManager) onRoleChange(regionID uint64, raftState raft.StateType) {
	if raftState == raft.StateLeader {
		mm.ingestRegionFiles(regionID)
	} else if raftState == raft.StateFollower {
		mm.clearFiles(regionID)
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
	version    uint64
	mu         sync.Mutex
	state      unsafe.Pointer
	appliedIdx uint64
	stableIdx  uint64

	snapTS   uint64
	startKey []byte
	endKey   []byte
	memTbls  []*memtable.CFTable
	l0s      []*protos.L0Create
	cfs      []cfFileMeta
}

func newRegionFileMeta(region *metapb.Region, numCFs, maxLevel int) *regionFileMeta {
	rfm := &regionFileMeta{
		id: region.Id,
		version: region.RegionEpoch.Version,
		startKey: RawStartKey(region),
		endKey: RawEndKey(region),
	}
	rfm.state = unsafe.Pointer(new(loadingState))
	rfm.cfs = make([]cfFileMeta, numCFs)
	for cf := 0; cf < numCFs; cf++ {
		rfm.cfs[cf] = cfFileMeta{levels: make([]levelFileMeta, maxLevel-1)}
	}
	return rfm
}

func (rfm *regionFileMeta) removeFile(id uint64) {
	var i int
	for ; i < len(rfm.l0s); i++ {
		if rfm.l0s[i].ID == id {
			break
		}
	}
	if i < len(rfm.l0s) {
		rfm.l0s = append(rfm.l0s[i:], rfm.l0s[:i+1]...)
		return
	}
	for _, cf := range rfm.cfs {
		for _, l := range cf.levels {
			for _, f := range l.files {
				if f.ID == id {
					l.files = append(l.files[i:], l.files[i+1:]...)
					return
				}
			}
		}
	}
	log.Error("region file meta remove file not found", zap.Uint64("fileID", id))
	return
}

func (rfm *regionFileMeta) getLoadingState() *loadingState {
	return (*loadingState)(atomic.LoadPointer(&rfm.state))
}

func (rfm *regionFileMeta) addL0(meta *protos.L0Create) {
	for i := 0; i < len(rfm.l0s); i++ {
		if rfm.l0s[i].ID == meta.ID {
			// The meta is already added.
			return
		}
	}
	rfm.l0s = append(rfm.l0s, meta)
}

func (rfm *regionFileMeta) addFile(meta *protos.TableCreate) {
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
		L0Creates:    make([]*protos.L0Create, 0, l0Cnt),
		TableCreates: make([]*protos.TableCreate, 0, lnCnt),
	}
	rfm.mu.Lock()
	e.StartKey = rfm.startKey
	e.EndKey = rfm.endKey
	e.L0Creates = append(e.L0Creates, rfm.l0s...)
	for _, cf := range rfm.cfs {
		for _, lvl := range cf.levels {
			e.TableCreates = append(e.TableCreates, lvl.files...)
		}
	}
	rfm.mu.Unlock()
	return e
}

type cfFileMeta struct {
	levels []levelFileMeta
}

type levelFileMeta struct {
	files []*protos.TableCreate
}

type loadingState struct {
	ok  bool
	wg  *sync.WaitGroup
	err error
}
*/
