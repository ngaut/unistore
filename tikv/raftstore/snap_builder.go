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
	"os"

	"github.com/coocood/badger"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/rocksdb"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/util/codec"
)

func newSnapBuilder(cfFiles []*CFFile, snap *regionSnapshot, region *metapb.Region) (*snapBuilder, error) {
	b := new(snapBuilder)
	b.cfFiles = cfFiles
	b.endKey = rawRegionKey(region.EndKey)
	b.txn = snap.txn
	itOpt := badger.DefaultIteratorOptions
	itOpt.AllVersions = true
	b.dbIterator = b.txn.NewIterator(itOpt)
	startKey := rawDataStartKey(region.StartKey)

	b.dbIterator.Seek(startKey)
	if b.dbIterator.Valid() && !b.reachEnd(b.dbIterator.Item().Key()) {
		b.curDBKey = b.dbIterator.Item().Key()
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
	txn             *badger.Txn
	lockIterator    *lockstore.Iterator
	dbIterator      *badger.Iterator
	curLockKey      []byte
	curDBKey        []byte
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
		b.txn.Discard()
	}()
	for {
		var err error
		if b.curLockKey != nil && b.curDBKey != nil {
			if bytes.Compare(b.curLockKey, b.curDBKey) <= 0 {
				err = b.addLockEntry()
			} else {
				err = b.addDBEntry()
			}
		} else if b.curLockKey != nil {
			err = b.addLockEntry()
		} else if b.curDBKey != nil {
			err = b.addDBEntry()
		} else {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (b *snapBuilder) reachEnd(key []byte) bool {
	if len(b.endKey) == 0 {
		return false
	}
	return bytes.Compare(key, b.endKey) >= 0
}

func (b *snapBuilder) addLockEntry() error {
	lockCFKey := DataKey(b.curLockKey)
	l := mvcc.DecodeLock(b.lockIterator.Value())
	lockCFVal := new(lockCFValue)
	lockCFVal.lockType = l.Op
	lockCFVal.startTS = l.StartTS
	lockCFVal.primary = l.Primary
	lockCFVal.ttl = uint64(l.TTL)
	if len(l.Value) <= shortValueMaxLen {
		lockCFVal.shortVal = l.Value
	} else {
		defaultCFKey := encodeRocksDBSSTKey(b.curLockKey, l.StartTS)
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
	err = b.addSSTKey(b.curDBKey, meta.StartTS(), meta.CommitTS(), val)
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

func (b *snapBuilder) addSSTKey(key []byte, startTS, commitTS uint64, val []byte) error {
	writeCFKey := encodeRocksDBSSTKey(key, commitTS)
	writeCFVal := new(writeCFValue)
	if len(val) == 0 {
		writeCFVal.writeType = byte(kvrpcpb.Op_Del)
	} else {
		writeCFVal.writeType = byte(kvrpcpb.Op_Put)
	}
	writeCFVal.startTS = startTS
	if len(val) <= shortValueMaxLen {
		writeCFVal.shortValue = val
	} else {
		defaultCFKey := encodeRocksDBSSTKey(b.curDBKey, startTS)
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

func isLatestKeyAndOldKeySame(lastestKey, oldKey []byte) bool {
	return len(oldKey)-len(lastestKey) == 8 && bytes.Equal(lastestKey[1:], oldKey[1:len(lastestKey)])
}
