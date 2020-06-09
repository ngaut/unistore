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
	"bytes"
	"math"
	"os"

	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/rocksdb"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/badger"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/util/codec"
)

func newSnapBuilder(cfFiles []*CFFile, snap *regionSnapshot, region *metapb.Region) (*snapBuilder, error) {
	b := new(snapBuilder)
	b.cfFiles = cfFiles
	b.endKey = RawEndKey(region)
	b.extraEndKey = mvcc.EncodeExtraTxnStatusKey(b.endKey, 0)
	b.txn = snap.txn
	itOpt := badger.DefaultIteratorOptions
	itOpt.AllVersions = true
	b.dbIterator = b.txn.NewIterator(itOpt)
	// extraIterator doesn't need to read all versions because startTS is encoded in the key.
	b.extraIterator = b.txn.NewIterator(badger.DefaultIteratorOptions)
	startKey := RawStartKey(region)

	b.dbIterator.Seek(startKey)
	if b.dbIterator.Valid() && !b.reachEnd(b.dbIterator.Item().Key()) {
		b.curDBKey = b.dbIterator.Item().Key()
	}
	b.extraIterator.Seek(mvcc.EncodeExtraTxnStatusKey(startKey, math.MaxUint64))
	if b.extraIterator.Valid() && !b.reachExtraEnd(b.extraIterator.Item().Key()) {
		b.curExtraKey = mvcc.DecodeExtraTxnStatusKey(b.extraIterator.Item().Key())
	}

	b.lockIterator = snap.lockSnap.NewIterator()
	b.lockIterator.Seek(startKey)
	if b.lockIterator.Valid() && !b.reachEnd(b.lockIterator.Key()) {
		b.curLockKey = b.lockIterator.Key()
	}
	lockCFFile := cfFiles[lockCFIdx].File
	if lockCFFile == nil {
		return nil, errors.New("lock CF file is nil")
	}
	b.lockCFWriter = cfFiles[lockCFIdx].File
	if cfFiles[defaultCFIdx].SstWriter == nil {
		return nil, errors.New("default CF SstWriter is nil")
	}
	b.defaultCFWriter = cfFiles[defaultCFIdx].SstWriter
	if cfFiles[writeCFIdx].SstWriter == nil {
		return nil, errors.New("write CF SstWriter is nil")
	}
	b.writeCFWriter = cfFiles[writeCFIdx].SstWriter
	return b, nil
}

// snapBuilder builds snapshot files.
// TODO: handle rollbacks and locks the region later.
type snapBuilder struct {
	endKey          []byte
	extraEndKey     []byte
	txn             *badger.Txn
	lockIterator    *lockstore.Iterator
	dbIterator      *badger.Iterator
	extraIterator   *badger.Iterator
	curLockKey      []byte
	curDBKey        []byte
	curExtraKey     []byte
	lockCFWriter    *os.File
	defaultCFWriter *rocksdb.SstFileWriter
	writeCFWriter   *rocksdb.SstFileWriter
	cfFiles         []*CFFile
	buf             []byte
	buf2            []byte
	kvCount         int
	size            int
}

func (b *snapBuilder) build() error {
	defer func() {
		b.dbIterator.Close()
		b.extraIterator.Close()
		b.txn.Discard()
	}()
	for {
		var err error
		switch b.currentKeyType() {
		case currentKeyDB:
			if len(b.curDBKey) == 0 {
				return nil
			}
			err = b.addDBEntry()
		case currentKeyLock:
			if len(b.curLockKey) == 0 {
				return nil
			}
			err = b.addLockEntry()
		case currentKeyExtra:
			if len(b.curExtraKey) == 0 {
				return nil
			}
			err = b.addExtraEntry()
		}
		if err != nil {
			return err
		}
	}
}

const (
	currentKeyDB = iota
	currentKeyLock
	currentKeyExtra
)

func (b *snapBuilder) currentKeyType() (keyType int) {
	curKey := b.curDBKey
	if len(curKey) == 0 || (len(b.curLockKey) > 0 && bytes.Compare(b.curLockKey, curKey) < 0) {
		keyType = currentKeyLock
	}
	if len(curKey) == 0 || (len(b.curExtraKey) > 0 && bytes.Compare(b.curExtraKey, curKey) < 0) {
		keyType = currentKeyExtra
	}
	return
}

func (b *snapBuilder) reachEnd(key []byte) bool {
	return bytes.Compare(key, b.endKey) >= 0
}

func (b *snapBuilder) reachExtraEnd(key []byte) bool {
	return bytes.Compare(key, b.extraEndKey) >= 0
}

func (b *snapBuilder) addLockEntry() error {
	lockCFKey := encodeRocksDBSSTKey(b.curLockKey, nil)
	l := mvcc.DecodeLock(b.lockIterator.Value())
	lockCFVal := new(lockCFValue)
	lockCFVal.lockType = l.Op
	lockCFVal.startTS = l.StartTS
	lockCFVal.primary = l.Primary
	lockCFVal.ttl = uint64(l.TTL)
	if len(l.Value) <= shortValueMaxLen {
		lockCFVal.shortVal = l.Value
	} else {
		defaultCFKey := encodeRocksDBSSTKey(b.curLockKey, &l.StartTS)
		err := b.defaultCFWriter.Put(defaultCFKey, l.Value)
		if err != nil {
			return err
		}
		b.cfFiles[defaultCFIdx].KVCount++
		b.size += len(defaultCFKey) + len(l.Value)
		b.kvCount++
	}
	b.buf = codec.EncodeCompactBytes(b.buf[:0], lockCFKey)
	_, err := b.lockCFWriter.Write(b.buf)
	if err != nil {
		return err
	}
	b.size += len(b.buf)
	b.buf2 = encodeLockCFValue(lockCFVal, b.buf2[:0])
	b.buf = codec.EncodeCompactBytes(b.buf[:0], b.buf2)
	_, err = b.lockCFWriter.Write(b.buf)
	if err != nil {
		return err
	}
	b.cfFiles[lockCFIdx].KVCount++
	b.size += len(b.buf)
	b.kvCount++

	b.lockIterator.Next()
	if b.lockIterator.Valid() && !b.reachEnd(b.lockIterator.Key()) {
		b.curLockKey = b.lockIterator.Key()
	} else {
		b.curLockKey = nil
	}
	return nil
}

func (b *snapBuilder) addDBEntry() error {
	item := b.dbIterator.Item()
	val, err := item.Value()
	if err != nil {
		return err
	}
	meta := mvcc.DBUserMeta(item.UserMeta())
	writeType := byte(kvrpcpb.Op_Put)
	if len(val) == 0 {
		writeType = byte(kvrpcpb.Op_Del)
	}
	if len(meta) == 0 {
		// delete range entry.
		meta = mvcc.NewDBUserMeta(item.Version(), item.Version())
	}
	err = b.addSSTKey(b.curDBKey, meta.StartTS(), meta.CommitTS(), val, writeType)
	if err != nil {
		return err
	}
	b.dbIterator.Next()
	if b.dbIterator.Valid() && !b.reachEnd(b.dbIterator.Item().Key()) {
		b.curDBKey = b.dbIterator.Item().Key()
	} else {
		b.curDBKey = nil
	}
	return nil
}

func (b *snapBuilder) addExtraEntry() error {
	item := b.extraIterator.Item()
	meta := mvcc.DBUserMeta(item.UserMeta())
	var sstCommitTS uint64
	var writeType byte
	if meta.CommitTS() == 0 {
		writeType = byte(kvrpcpb.Op_Rollback)
		sstCommitTS = meta.StartTS()
	} else {
		writeType = byte(kvrpcpb.Op_Lock)
		sstCommitTS = meta.CommitTS()
	}
	err := b.addSSTKey(b.curExtraKey, meta.StartTS(), sstCommitTS, nil, writeType)
	if err != nil {
		return err
	}
	b.curExtraKey = nil
	b.extraIterator.Next()
	if b.extraIterator.Valid() && !b.reachExtraEnd(b.extraIterator.Item().Key()) {
		b.curExtraKey = mvcc.DecodeExtraTxnStatusKey(b.extraIterator.Item().Key())
	}
	return nil
}

func (b *snapBuilder) addSSTKey(key []byte, startTS, commitTS uint64, val []byte, writeType byte) error {
	writeCFKey := encodeRocksDBSSTKey(key, &commitTS)
	writeCFVal := new(writeCFValue)
	writeCFVal.writeType = writeType
	writeCFVal.startTS = startTS
	if len(val) <= shortValueMaxLen {
		writeCFVal.shortValue = val
	} else {
		defaultCFKey := encodeRocksDBSSTKey(b.curDBKey, &startTS)
		err := b.defaultCFWriter.Put(defaultCFKey, val)
		if err != nil {
			return err
		}
		b.cfFiles[defaultCFIdx].KVCount++
		b.kvCount++
		b.size += len(defaultCFKey) + len(val)
	}
	encodedWriteCFVal := encodeWriteCFValue(writeCFVal)
	err := b.writeCFWriter.Put(writeCFKey, encodedWriteCFVal)
	if err != nil {
		return err
	}
	b.cfFiles[writeCFIdx].KVCount++
	b.size += len(writeCFKey) + len(encodedWriteCFVal)
	b.kvCount++
	return nil
}
