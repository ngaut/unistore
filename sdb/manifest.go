package sdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ngaut/unistore/sdbpb"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

const (
	// ManifestFilename is the filename for the manifest file.
	ManifestFilename                  = "MANIFEST"
	manifestRewriteFilename           = "MANIFEST-REWRITE"
	manifestDeletionsRewriteThreshold = 10000
	manifestDeletionsRatio            = 10
	// The magic version number.
	magicVersion = 4
)

// Has to be 4 bytes.  The value can never change, ever, anyway.
var magicText = [4]byte{'B', 'd', 'g', 'r'}

// Manifest
// The manifest file is used to restore the tree
type Manifest struct {
	dir         string
	shards      map[uint64]*ShardMeta
	globalFiles map[uint64]fileMeta
	lastID      uint64
	dataVersion uint64
	fd          *os.File
	deletions   int
	creations   int

	// Guards appends, which includes access to the manifest field.
	appendLock sync.Mutex
	// We make this configurable so that unit tests can hit rewrite() code quickly
	deletionsRewriteThreshold int
	orc                       *oracle
}

type ShardMeta struct {
	ID    uint64
	Ver   uint64
	Start []byte
	End   []byte
	// fid -> level
	files map[uint64]int
	// properties in ShardMeta is only updated on every mem-table flush, it's different than properties in the shard
	// which is updated on every write operation.
	properties *properties
	preSplit   *sdbpb.PreSplit
	split      *sdbpb.Split
	splitState sdbpb.SplitState
	commitTS   uint64
	parent     *ShardMeta
	recovered  bool
}

func (si *ShardMeta) FileLevel(fid uint64) (int, bool) {
	level, ok := si.files[fid]
	return level, ok
}

// ShardLevel is the struct that contains shard id and level id,
type LevelCF struct {
	Level uint16
	CF    uint16
}

var globalShardEndKey = []byte{255, 255, 255, 255, 255, 255, 255, 255}

func OpenManifest(dir string) (*Manifest, error) {
	path := filepath.Join(dir, ManifestFilename)
	fd, err := y.OpenExistingFile(path, 0) // We explicitly sync in addChanges, outside the lock.
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		m := &Manifest{
			dir:         dir,
			shards:      map[uint64]*ShardMeta{},
			lastID:      1,
			globalFiles: map[uint64]fileMeta{},
		}
		err = m.rewrite()
		if err != nil {
			return nil, err
		}
		return m, err
	}
	m, truncOffset, err := ReplayManifestFile(fd)
	if err != nil {
		return nil, err
	}
	// Truncate file so we don't have a half-written entry at the end.
	if err = fd.Truncate(truncOffset); err != nil {
		_ = fd.Close()
		return nil, err
	}
	if _, err = fd.Seek(0, io.SeekEnd); err != nil {
		_ = fd.Close()
		return nil, err
	}
	return m, nil
}

func (m *Manifest) toChangeSets() ([]*sdbpb.ChangeSet, error) {
	var shards []*sdbpb.ChangeSet
	for id := range m.shards {
		cs, err := m.toChangeSet(id)
		if err != nil {
			return nil, err
		}
		shards = append(shards, cs)
	}
	return shards, nil
}

var ErrHasParent = errors.New("has parent")

func (m *Manifest) toChangeSet(shardID uint64) (*sdbpb.ChangeSet, error) {
	shard := m.shards[shardID]
	if shard.parent != nil {
		return nil, ErrHasParent
	}
	cs := &sdbpb.ChangeSet{
		DataVer:  m.dataVersion,
		ShardID:  shard.ID,
		ShardVer: shard.Ver,
		State:    shard.splitState,
	}
	if shard.preSplit != nil {
		cs.PreSplit = shard.preSplit
	}
	shardSnap := &sdbpb.Snapshot{
		Start:      shard.Start,
		End:        shard.End,
		Properties: shard.properties.toPB(shard.ID),
		CommitTS:   shard.commitTS,
	}
	cs.Snapshot = shardSnap
	for fid := range shard.files {
		fileMeta := m.globalFiles[fid]
		if fileMeta.level == 0 {
			shardSnap.L0Creates = append(shardSnap.L0Creates, &sdbpb.L0Create{
				ID:         fid,
				Properties: nil, // Store properties in ShardCreate.
			})
		} else {
			shardSnap.TableCreates = append(shardSnap.TableCreates, &sdbpb.TableCreate{
				ID:       fid,
				Level:    fileMeta.level,
				CF:       fileMeta.cf,
				Smallest: fileMeta.smallest,
				Biggest:  fileMeta.biggest,
			})
		}
	}
	return cs, nil
}

func (m *Manifest) rewrite() error {
	log.Info("rewrite manifest")
	changeSets, err := m.toChangeSets()
	if err != nil {
		if err == ErrHasParent {
			return nil
		}
		return err
	}
	changeSetsBuf := make([]byte, 8)
	copy(changeSetsBuf, magicText[:])
	binary.BigEndian.PutUint32(changeSetsBuf[4:], magicVersion)
	var creations int
	for _, cs := range changeSets {
		data, _ := cs.Marshal()
		creations += len(cs.Snapshot.L0Creates) + len(cs.Snapshot.TableCreates)
		changeSetsBuf = appendChecksumPacket(changeSetsBuf, data)
	}
	if m.fd != nil {
		m.fd.Close()
	}
	m.fd, err = rewriteManifest(changeSetsBuf, m.dir)
	if err != nil {
		return err
	}
	m.creations = creations
	m.deletions = 0
	return nil
}

func rewriteManifest(changeBuf []byte, dir string) (*os.File, error) {
	rewritePath := filepath.Join(dir, manifestRewriteFilename)
	// We explicitly sync.
	fp, err := y.OpenTruncFile(rewritePath, false)
	if err != nil {
		return nil, err
	}
	y.Assert(binary.BigEndian.Uint32(changeBuf[4:]) == magicVersion)
	if _, err := fp.Write(changeBuf); err != nil {
		fp.Close()
		return nil, err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return nil, err
	}

	// In Windows the files should be closed before doing a Rename.
	if err = fp.Close(); err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(dir, ManifestFilename)
	if err := os.Rename(rewritePath, manifestPath); err != nil {
		return nil, err
	}
	fp, err = y.OpenExistingFile(manifestPath, 0)
	if err != nil {
		return nil, err
	}
	if _, err := fp.Seek(0, io.SeekEnd); err != nil {
		fp.Close()
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		fp.Close()
		return nil, err
	}
	return fp, nil
}

// When you create or delete a file, you have to ensure the directory entry for the file is synced
// in order to guarantee the file is visible (if the system crashes).  (See the man page for fsync,
// or see https://github.com/coreos/etcd/issues/6368 for an example.)
func syncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s.", dir)
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "While syncing directory: %s.", dir)
	}
	return errors.Wrapf(closeErr, "While closing directory: %s.", dir)
}

func appendMagicHeader(buf []byte) []byte {
	buf = append(buf, magicText[:]...)
	return append(buf, 0, 0, 0, magicVersion)
}

func appendChecksumPacket(buf, packet []byte) []byte {
	var lenCrcBuf [8]byte
	binary.BigEndian.PutUint32(lenCrcBuf[0:4], uint32(len(packet)))
	binary.BigEndian.PutUint32(lenCrcBuf[4:8], crc32.Checksum(packet, y.CastagnoliCrcTable))
	buf = append(buf, lenCrcBuf[:]...)
	return append(buf, packet...)
}

func (m *Manifest) Close() error {
	return m.fd.Close()
}

func (m *Manifest) ApplyChangeSet(cs *sdbpb.ChangeSet) error {
	if m.dataVersion < cs.DataVer {
		m.dataVersion = cs.DataVer
	}
	if cs.Snapshot != nil {
		m.applySnapshot(cs)
		return nil
	}
	shardInfo := m.shards[cs.ShardID]
	if shardInfo == nil {
		return errors.WithStack(errShardNotFound)
	}
	y.Assert(shardInfo.Ver == cs.ShardVer)
	if cs.Flush != nil {
		m.applyFlush(cs, shardInfo)
		if cs.State == sdbpb.SplitState_PRE_SPLIT_FLUSH_DONE {
			shardInfo.splitState = sdbpb.SplitState_PRE_SPLIT_FLUSH_DONE
			if shardInfo.preSplit != nil && shardInfo.preSplit.MemProps != nil {
				shardInfo.preSplit.MemProps = nil
			}
		}
		return nil
	}
	if cs.Compaction != nil {
		m.applyCompaction(cs, shardInfo)
		return nil
	}
	if cs.PreSplit != nil {
		y.Assert(cs.PreSplit.MemProps != nil)
		shardInfo.preSplit = cs.PreSplit
		shardInfo.splitState = sdbpb.SplitState_PRE_SPLIT
		return nil
	}
	if cs.SplitFiles != nil {
		m.applySplitFiles(cs, shardInfo)
		shardInfo.splitState = sdbpb.SplitState_SPLIT_FILE_DONE
		return nil
	}
	if cs.Split != nil {
		m.applySplit(cs.ShardID, cs.Split)
		return nil
	}
	if cs.ShardDelete {
		delete(m.shards, cs.ShardID)
	}
	return nil
}

func (m *Manifest) applySnapshot(cs *sdbpb.ChangeSet) {
	log.S().Infof("%d:%d apply snapshot", cs.ShardID, cs.ShardVer)
	snap := cs.Snapshot
	shard := &ShardMeta{
		ID:         cs.ShardID,
		Ver:        cs.ShardVer,
		Start:      snap.Start,
		End:        snap.End,
		files:      map[uint64]int{},
		properties: newProperties().applyPB(snap.Properties),
		splitState: cs.State,
		commitTS:   snap.CommitTS,
	}
	if len(cs.Snapshot.SplitKeys) > 0 {
		shard.preSplit = &sdbpb.PreSplit{Keys: cs.Snapshot.SplitKeys}
	}
	for _, l0 := range snap.L0Creates {
		m.addFile(l0.ID, -1, 0, l0.Start, l0.End, shard)
	}
	for _, tbl := range snap.TableCreates {
		m.addFile(tbl.ID, tbl.CF, tbl.Level, tbl.Smallest, tbl.Biggest, shard)
	}
	m.shards[cs.ShardID] = shard
}

func (m *Manifest) applyFlush(cs *sdbpb.ChangeSet, shardInfo *ShardMeta) {
	log.S().Infof("%d:%d apply flush", cs.ShardID, cs.ShardVer)
	shardInfo.commitTS = cs.Flush.CommitTS
	shardInfo.parent = nil
	l0 := cs.Flush.L0Create
	if l0 != nil {
		if l0.Properties != nil {
			for i, key := range l0.Properties.Keys {
				shardInfo.properties.set(key, l0.Properties.Values[i])
			}
		}
		m.addFile(l0.ID, -1, 0, l0.Start, l0.End, shardInfo)
	}
}

func (m *Manifest) addFile(fid uint64, cf int32, level uint32, smallest, biggest []byte, shardInfo *ShardMeta) {
	log.S().Infof("manifest add file %d l%d smalleset %v biggest %v", fid, level, smallest, biggest)
	if fid > m.lastID {
		m.lastID = fid
	}
	m.creations++
	shardInfo.files[fid] = int(level)
	m.globalFiles[fid] = fileMeta{cf: cf, level: level, smallest: smallest, biggest: biggest}
}

func (m *Manifest) deleteFile(fid uint64, shardInfo *ShardMeta) {
	log.S().Infof("%d:%d manifest del file %d", shardInfo.ID, shardInfo.Ver, fid)
	m.deletions++
	delete(shardInfo.files, fid)
	delete(m.globalFiles, fid)
}

func (m *Manifest) applyCompaction(cs *sdbpb.ChangeSet, shardInfo *ShardMeta) {
	log.S().Infof("%d:%d apply compaction", cs.ShardID, cs.ShardVer)
	for _, id := range cs.Compaction.TopDeletes {
		m.deleteFile(id, shardInfo)
	}
	for _, id := range cs.Compaction.BottomDeletes {
		m.deleteFile(id, shardInfo)
	}
	for _, create := range cs.Compaction.TableCreates {
		m.addFile(create.ID, create.CF, create.Level, create.Smallest, create.Biggest, shardInfo)
	}
}

func (m *Manifest) applySplitFiles(cs *sdbpb.ChangeSet, shardInfo *ShardMeta) {
	log.S().Infof(" %d:%d apply split files", shardInfo.ID, shardInfo.Ver)
	for _, id := range cs.SplitFiles.TableDeletes {
		m.deleteFile(id, shardInfo)
	}
	for _, l0 := range cs.SplitFiles.L0Creates {
		m.addFile(l0.ID, -1, 0, l0.Start, l0.End, shardInfo)
	}
	for _, tbl := range cs.SplitFiles.TableCreates {
		m.addFile(tbl.ID, tbl.CF, tbl.Level, tbl.Smallest, tbl.Biggest, shardInfo)
	}
}

func (m *Manifest) applySplit(shardID uint64, split *sdbpb.Split) {
	old := m.shards[shardID]
	log.S().Infof("%d:%d apply split, files %v", old.ID, old.Ver, old.files)
	newShards := make([]*ShardMeta, len(split.NewShards))
	newVer := old.Ver + uint64(len(newShards)) - 1
	for i := 0; i < len(split.NewShards); i++ {
		startKey, endKey := getSplittingStartEnd(old.Start, old.End, split.Keys, i)
		id := split.NewShards[i].ShardID
		if id == old.ID {
			old.split = split
		}
		shardInfo := &ShardMeta{
			ID:         id,
			Ver:        newVer,
			Start:      startKey,
			End:        endKey,
			files:      map[uint64]int{},
			properties: newProperties().applyPB(split.NewShards[i]),
			parent:     old,
		}
		m.shards[id] = shardInfo
		newShards[i] = shardInfo
	}
	for fid := range old.files {
		fileMeta := m.globalFiles[fid]
		shardIdx := getSplitShardIndex(split.Keys, fileMeta.smallest)
		newShards[shardIdx].files[fid] = int(fileMeta.level)
	}
	for _, nShard := range newShards {
		log.S().Infof("new shard %d:%d smallest %v biggest %v files %v",
			nShard.ID, nShard.Ver, nShard.Start, nShard.End, nShard.files)
	}
}

var errDupChange = errors.New("duplicated change")

func (m *Manifest) writeChangeSet(changeSet *sdbpb.ChangeSet) error {
	// Maybe we could use O_APPEND instead (on certain file systems)
	m.appendLock.Lock()
	defer m.appendLock.Unlock()
	if m.isDuplicatedChange(changeSet) {
		return errDupChange
	}
	changeSet.DataVer = m.orc.commitTs()
	buf, err := changeSet.Marshal()
	if err != nil {
		return err
	}
	// Rewrite manifest if it'd shrink by 1/10 and it's big enough to care
	if m.deletions > m.deletionsRewriteThreshold &&
		m.deletions > manifestDeletionsRatio*(m.creations-m.deletions) {
		log.S().Infof("deletions %d createions %d", m.deletions, m.creations)
		if err = m.rewrite(); err != nil {
			return err
		}
	} else {
		buf = appendChecksumPacket([]byte{}, buf)
		if _, err = m.fd.Write(buf); err != nil {
			return err
		}
	}
	err = m.fd.Sync()
	if err != nil {
		return err
	}
	if err = m.ApplyChangeSet(changeSet); err != nil {
		return err
	}
	return nil
}

func (m *Manifest) isDuplicatedChange(change *sdbpb.ChangeSet) bool {
	meta, ok := m.shards[change.ShardID]
	if !ok {
		return false
	}
	if flush := change.Flush; flush != nil {
		if meta.parent != nil {
			return false
		}
		if flush.L0Create == nil {
			return meta.splitState >= change.State
		}
		return meta.commitTS >= flush.CommitTS
	}
	if comp := change.Compaction; comp != nil {
		// TODO: It is a temporary solution that can fail in very rare case.
		// It is possible that a duplicated compaction's all new tables are removed by future compaction.
		for _, tbl := range comp.TableCreates {
			level, ok := meta.FileLevel(tbl.ID)
			if ok && level > int(change.Compaction.Level) {
				return true
			}
		}
	}
	if splitFiles := change.SplitFiles; splitFiles != nil {
		return meta.splitState == change.State
	}
	return false
}

type fileMeta struct {
	cf       int32
	level    uint32
	smallest []byte
	biggest  []byte
}

type countingReader struct {
	wrapped *bufio.Reader
	count   int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.wrapped.Read(p)
	r.count += int64(n)
	return
}

func (r *countingReader) ReadByte() (b byte, err error) {
	b, err = r.wrapped.ReadByte()
	if err == nil {
		r.count++
	}
	return
}

var (
	errBadMagic = errors.New("manifest has bad magic")
)

func ReplayManifestFile(fp *os.File) (ret *Manifest, truncOffset int64, err error) {
	log.Info("replay manifest")
	r := &countingReader{wrapped: bufio.NewReader(fp)}
	if err = readManifestMagic(r); err != nil {
		return nil, 0, err
	}
	ret = &Manifest{
		shards:      map[uint64]*ShardMeta{},
		globalFiles: map[uint64]fileMeta{},
		fd:          fp,
	}
	var offset int64
	for {
		offset = r.count
		var buf []byte
		buf, err = readChecksumPacket(r)
		if err != nil {
			return nil, 0, err
		}
		if len(buf) == 0 {
			break
		}
		changeSet := new(sdbpb.ChangeSet)
		err = changeSet.Unmarshal(buf)
		if err != nil {
			return nil, 0, err
		}
		err = ret.ApplyChangeSet(changeSet)
		if err != nil {
			return nil, 0, err
		}
	}
	return ret, offset, nil
}

func readChecksumPacket(r io.Reader) ([]byte, error) {
	var lenCrcBuf [8]byte
	_, err := io.ReadFull(r, lenCrcBuf[:])
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, nil
		}
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenCrcBuf[0:4])
	var buf = make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, nil
		}
		return nil, err
	}
	if crc32.Checksum(buf, y.CastagnoliCrcTable) != binary.BigEndian.Uint32(lenCrcBuf[4:8]) {
		return nil, nil
	}
	return buf, nil
}

func readManifestMagic(r io.Reader) error {
	var magicBuf [8]byte
	if _, err := io.ReadFull(r, magicBuf[:]); err != nil {
		return errors.Wrap(errBadMagic, err.Error())
	}
	if !bytes.Equal(magicBuf[0:4], magicText[:]) {
		return errors.Wrap(errBadMagic, fmt.Sprintf("magic not match got %x expect %x", magicBuf[:4], magicText))
	}
	version := binary.BigEndian.Uint32(magicBuf[4:8])
	if version != magicVersion {
		return fmt.Errorf("manifest has unsupported version: %d (we support %d)", version, magicVersion)
	}
	return nil
}

func newChangeSet(shard *Shard) *sdbpb.ChangeSet {
	return &sdbpb.ChangeSet{
		ShardID:  shard.ID,
		ShardVer: shard.Ver,
		State:    shard.GetSplitState(),
	}
}