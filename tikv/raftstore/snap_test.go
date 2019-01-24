package raftstore

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"

	"github.com/coocood/badger"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	rspb "github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	snapTestKey        = []byte("tkey")
	snapTestKeyOld     = encodeOldKey(snapTestKey, 100)
	regionTestBegin    = []byte("ta")
	regionTestBeginOld = []byte("ua")
	regionTestEnd      = []byte("tz")
	regionTestEndOld   = []byte("uz")
)

const (
	testWriteBatchSize = 10 * 1024 * 1024
)

type dummyDeleter struct{}

func (d *dummyDeleter) DeleteSnapshot(key *SnapKey, snapshot Snapshot, checkEntry bool) bool {
	snapshot.Delete()
	return true
}

func getKVCount(t *testing.T, db *DBSnapshot) int {
	count := 0
	err := db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(regionTestBegin); it.Valid(); it.Next() {
			if bytes.Compare(it.Item().Key(), regionTestEnd) >= 0 {
				break
			}
			count++
		}
		for it.Seek(regionTestBeginOld); it.Valid(); it.Next() {
			if bytes.Compare(it.Item().Key(), regionTestEndOld) >= 0 {
				break
			}
			count++
		}
		return nil
	})
	lockIterator := db.lockStore.NewIterator()
	for lockIterator.Seek(regionTestBegin); lockIterator.Valid(); lockIterator.Next() {
		if bytes.Compare(lockIterator.Key(), regionTestEnd) >= 0 {
			break
		}
		count++
	}
	assert.Nil(t, err)
	return count
}

func genTestRegion(regionID, storeID, peerID uint64) *metapb.Region {
	return &metapb.Region{
		Id:       regionID,
		StartKey: codec.EncodeBytes(nil, regionTestBegin),
		EndKey:   codec.EncodeBytes(nil, regionTestEnd),
		RegionEpoch: &metapb.RegionEpoch{
			Version: 1,
			ConfVer: 1,
		},
		Peers: []*metapb.Peer{
			{StoreId: storeID, Id: peerID},
		},
	}
}

func assertEqDB(t *testing.T, expected, actual *DBSnapshot) {
	expectedVal := getDBValue(t, expected.db, snapTestKey)
	actualVal := getDBValue(t, actual.db, snapTestKey)
	assert.Equal(t, expectedVal, actualVal)
	expectedVal = getDBValue(t, expected.db, snapTestKeyOld)
	actualVal = getDBValue(t, actual.db, snapTestKeyOld)
	assert.Equal(t, expectedVal, actualVal)
	expectedLock := expected.lockStore.Get(snapTestKey, nil)
	actualLock := actual.lockStore.Get(snapTestKey, nil)
	assert.Equal(t, expectedLock, actualLock)
}

func getDBValue(t *testing.T, db *badger.DB, key []byte) (val []byte) {
	require.Nil(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		require.Nil(t, err, string(key))
		val, err = item.Value()
		require.Nil(t, err)
		return nil
	}))
	return
}

func openDBSnapshot(t *testing.T, dir string) *DBSnapshot {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	require.Nil(t, err)
	lockStore := lockstore.NewMemStore(1024)
	rollbackStore := lockstore.NewMemStore(1024)
	return &DBSnapshot{
		db:            db,
		lockStore:     lockStore,
		rollbackStore: rollbackStore,
	}
}

func fillDBSnapshotData(t *testing.T, dbSnap *DBSnapshot) {
	// write some data.
	// Put an new version key and an old version key.
	err := dbSnap.db.Update(func(txn *badger.Txn) error {
		value := make([]byte, 32)
		require.Nil(t, txn.SetWithMetaSlice(snapTestKey, value, mvcc.NewDBUserMeta(150, 200)))
		oldUseMeta := mvcc.NewDBUserMeta(50, 100).ToOldUserMeta(200)
		require.Nil(t, txn.SetWithMetaSlice(snapTestKeyOld, make([]byte, 128), oldUseMeta))
		return nil
	})
	require.Nil(t, err)
	lockVal := &mvcc.MvccLock{
		MvccLockHdr: mvcc.MvccLockHdr{
			StartTS:    250,
			TTL:        100,
			Op:         byte(kvrpcpb.Op_Put),
			PrimaryLen: uint16(len(snapTestKey)),
			HasOldVer:  true,
		},
		Primary: snapTestKey,
		Value:   make([]byte, 128),
		OldVal:  make([]byte, 32),
		OldMeta: mvcc.NewDBUserMeta(150, 200),
	}
	dbSnap.lockStore.Insert(snapTestKey, lockVal.MarshalBinary())
}

func TestSnapGenMeta(t *testing.T) {
	cfFiles := make([]*CFFile, 0, len(snapshotCFs))
	for i, cf := range snapshotCFs {
		f := &CFFile{
			CF:       cf,
			Size:     100 * uint64(i+1),
			Checksum: 1000 * uint32(i+1),
		}
		cfFiles = append(cfFiles, f)
	}
	meta, err := genSnapshotMeta(cfFiles)
	require.Nil(t, err)
	for i, cfFileMeta := range meta.CfFiles {
		assert.Equal(t, cfFileMeta.Cf, cfFiles[i].CF)
		assert.Equal(t, cfFileMeta.Size_, cfFiles[i].Size)
		assert.Equal(t, cfFileMeta.Checksum, cfFiles[i].Checksum)
	}
}

func TestSnapDisplayPath(t *testing.T) {
	dir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	key := &SnapKey{1, 1, 2}
	prefix := fmt.Sprintf("%s_%s", snapGenPrefix, key)
	displayPath := getDisplayPath(dir, prefix)
	assert.NotEqual(t, displayPath, "")
}

func TestSnapFile(t *testing.T) {
	doTestSnapFile(t, true)
	doTestSnapFile(t, false)
}

func doTestSnapFile(t *testing.T, dbHasData bool) {
	regionID := uint64(1)
	region := genTestRegion(regionID, 1, 1)
	dir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	dbSnap := openDBSnapshot(t, dir)
	if dbHasData {
		fillDBSnapshotData(t, dbSnap)
	}

	snapDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(snapDir)
	key := &SnapKey{RegionID: regionID, Term: 1, Index: 1}
	sizeTrack := new(int64)
	deleter := &dummyDeleter{}
	s1, err := NewSnapForBuilding(snapDir, key, sizeTrack, deleter, nil)
	require.Nil(t, err)
	// Ensure that this snapshot file doesn't exist before being built.
	assert.False(t, s1.Exists())
	assert.Equal(t, int64(0), atomic.LoadInt64(sizeTrack))

	snapData := new(rspb.RaftSnapshotData)
	snapData.Region = region
	stat := new(SnapStatistics)
	assert.Nil(t, s1.Build(dbSnap, region, snapData, stat, deleter))
	require.NotNil(t, snapData.Meta)

	// Ensure that this snapshot file does exist after being built.
	assert.True(t, s1.Exists())
	totalSize := s1.TotalSize()
	// Ensure the `size_track` is modified correctly.
	size := atomic.LoadInt64(sizeTrack)
	assert.Equal(t, int64(totalSize), size)
	assert.Equal(t, int64(stat.Size), size)
	if dbHasData {
		assert.Equal(t, 3, getKVCount(t, dbSnap))
		// stat.KVCount is 5 because there are two extra default cf value.
		assert.Equal(t, 5, stat.KVCount)
	}

	// Ensure this snapshot could be read for sending.
	s2, err := NewSnapForSending(snapDir, key, sizeTrack, deleter)
	require.Nil(t, err, errors.ErrorStack(err))
	assert.True(t, s2.Exists())

	dstDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dstDir)

	s3, err := NewSnapForReceiving(dstDir, key, snapData.Meta, sizeTrack, deleter, nil)
	require.Nil(t, err)
	assert.False(t, s3.Exists())

	// Ensure snapshot data could be read out of `s2`, and write into `s3`.
	copySize, err := io.Copy(s3, s2)
	require.Nil(t, err)
	assert.Equal(t, copySize, size)
	assert.False(t, s3.Exists())
	assert.Nil(t, s3.Save())
	assert.True(t, s3.Exists())

	// Ensure the tracked size is handled correctly after receiving a snapshot.
	assert.Equal(t, atomic.LoadInt64(sizeTrack), size*2)

	// Ensure `delete()` works to delete the source snapshot.
	s2.Delete()
	assert.False(t, s2.Exists())
	assert.False(t, s1.Exists())
	assert.Equal(t, atomic.LoadInt64(sizeTrack), size)

	// Ensure a snapshot could be applied to DB.
	s4, err := NewSnapForApplying(dstDir, key, sizeTrack, deleter)
	require.Nil(t, err)
	assert.True(t, s4.Exists())

	dstDBDir, err := ioutil.TempDir("", "snapshot")
	require.Nil(t, err)
	defer os.RemoveAll(dstDBDir)

	dstDBSnap := openDBSnapshot(t, dstDBDir)
	abort := new(uint64)
	*abort = JobStatusRunning
	opts := ApplyOptions{
		DBSnap:    dstDBSnap,
		Region:    region,
		Abort:     abort,
		BatchSize: testWriteBatchSize,
	}
	err = s4.Apply(opts)
	require.Nil(t, err, errors.ErrorStack(err))

	// Ensure `delete()` works to delete the dest snapshot.
	s4.Delete()
	assert.False(t, s4.Exists())
	assert.False(t, s3.Exists())
	assert.Equal(t, atomic.LoadInt64(sizeTrack), int64(0))

	// Verify the data is correct after applying snapshot.
	if dbHasData {
		assertEqDB(t, dbSnap, dstDBSnap)
	}
}
