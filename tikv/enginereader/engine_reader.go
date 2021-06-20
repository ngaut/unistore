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

package enginereader

import (
	"bytes"
	"github.com/ngaut/unistore/engine"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func NewReader(startKey, endKey []byte, snap *engine.SnapAccess) *Reader {
	return &Reader{
		StartKey: startKey,
		EndKey:   endKey,
		snap:     snap,
	}
}

const (
	writeCF = 0
	lockCF  = 1
	extraCF = 2
)

// Reader reads data from Engine, for read-only requests, the locks must already be checked before Reader is created.
type Reader struct {
	StartKey  []byte
	EndKey    []byte
	snap      *engine.SnapAccess
	iter      *engine.Iterator
	lockIter  *engine.Iterator
	extraIter *engine.Iterator
	revIter   *engine.Iterator
}

// GetMvccInfoByKey fills MvccInfo reading committed keys from db
func (r *Reader) GetMvccInfoByKey(key []byte, isRowKey bool, mvccInfo *kvrpcpb.MvccInfo) error {
	it := r.GetIter()
	it.SetAllVersions(true)
	for it.Seek(key); it.Valid(); it.Next() {
		item := it.Item()
		if !bytes.Equal(item.Key(), key) {
			break
		}
		var val []byte
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		userMeta := mvcc.UserMeta(item.UserMeta())
		var tp kvrpcpb.Op
		if len(val) == 0 {
			tp = kvrpcpb.Op_Del
		} else {
			tp = kvrpcpb.Op_Put
		}
		mvccInfo.Writes = append(mvccInfo.Writes, &kvrpcpb.MvccWrite{
			Type:       tp,
			StartTs:    userMeta.StartTS(),
			CommitTs:   userMeta.CommitTS(),
			ShortValue: val,
		})
	}
	return nil
}

func (r *Reader) Get(key []byte, startTS uint64) ([]byte, error) {
	item, err := r.snap.Get(writeCF, key, startTS)
	if err != nil && err != engine.ErrKeyNotFound {
		return nil, errors.Trace(err)
	}
	if item == nil {
		return nil, nil
	}
	return item.Value()
}

func (r *Reader) GetIter() *engine.Iterator {
	if r.iter == nil {
		r.iter = r.snap.NewIterator(writeCF, false, false)
	}
	return r.iter
}

func (r *Reader) GetLock(key []byte, buf []byte) []byte {
	item, err := r.snap.Get(lockCF, key, 0)
	if err != nil && err != engine.ErrKeyNotFound {
		log.Error("get lock failed", zap.Error(err))
		return nil
	}
	if item == nil {
		return nil
	}
	val, _ := item.Value()
	return append(buf[:0], val...)
}

func (r *Reader) GetLockIter() *engine.Iterator {
	if r.lockIter == nil {
		r.lockIter = r.snap.NewIterator(lockCF, false, false)
	}
	return r.lockIter
}

func (r *Reader) GetExtraIter() *engine.Iterator {
	if r.extraIter == nil {
		r.extraIter = r.snap.NewIterator(extraCF, false, false)
	}
	return r.extraIter
}

func (r *Reader) getReverseIter() *engine.Iterator {
	if r.revIter == nil {
		r.revIter = r.snap.NewIterator(writeCF, true, false)
	}
	return r.revIter
}

type BatchGetFunc = func(key, value []byte, err error)

func (r *Reader) BatchGet(keys [][]byte, startTS uint64, f BatchGetFunc) {
	for _, key := range keys {
		item, err := r.snap.Get(0, key, startTS)
		if err == nil {
			val, _ := item.Value()
			f(key, val, nil)
		} else {
			f(key, nil, engine.ErrKeyNotFound)
		}
	}
	return
}

// ScanBreak is returnd by ScanFunc to break the scan loop.
var ScanBreak = errors.New("scan break")

// ScanFunc accepts key and value, should not keep reference to them.
// Returns ScanBreak will break the scan loop.
type ScanFunc = func(key, value []byte) error

// ScanProcessor process the key/value pair.
type ScanProcessor interface {
	// Process accepts key and value, should not keep reference to them.
	// Returns ScanBreak will break the scan loop.
	Process(key, value []byte) error
	// SkipValue returns if we can skip the value.
	SkipValue() bool
}

func exceedEndKey(current, endKey []byte) bool {
	if len(endKey) == 0 {
		return false
	}
	return bytes.Compare(current, endKey) >= 0
}

func (r *Reader) Scan(startKey, endKey []byte, limit int, startTS uint64, proc ScanProcessor) error {
	r.snap.SetManagedReadTS(startTS)
	skipValue := proc.SkipValue()
	iter := r.GetIter()
	var cnt int
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		if exceedEndKey(key, endKey) {
			break
		}
		var err error
		if item.IsEmpty() {
			continue
		}
		var val []byte
		if !skipValue {
			val, err = item.Value()
			if err != nil {
				return errors.Trace(err)
			}
		}
		err = proc.Process(key, val)
		if err != nil {
			if err == ScanBreak {
				break
			}
			return errors.Trace(err)
		}
		cnt++
		if cnt >= limit {
			break
		}
	}
	return nil
}

func (r *Reader) GetKeyByStartTs(startKey, endKey []byte, startTs uint64) ([]byte, error) {
	iter := r.GetIter()
	iter.SetAllVersions(true)
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		curItem := iter.Item()
		curKey := curItem.Key()
		if len(endKey) != 0 && bytes.Compare(curKey, endKey) >= 0 {
			break
		}
		meta := mvcc.UserMeta(curItem.UserMeta())
		if meta.StartTS() == startTs {
			return curItem.KeyCopy(nil), nil
		}
	}
	return nil, nil
}

// ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
func (r *Reader) ReverseScan(startKey, endKey []byte, limit int, startTS uint64, proc ScanProcessor) error {
	skipValue := proc.SkipValue()
	r.snap.SetManagedReadTS(startTS)
	iter := r.getReverseIter()
	var cnt int
	for iter.Seek(endKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		if bytes.Compare(key, startKey) < 0 {
			break
		}
		if cnt == 0 && bytes.Equal(key, endKey) {
			continue
		}
		var err error
		if item.IsEmpty() {
			continue
		}
		var val []byte
		if !skipValue {
			val, err = item.Value()
			if err != nil {
				return errors.Trace(err)
			}
		}
		err = proc.Process(key, val)
		if err != nil {
			if err == ScanBreak {
				break
			}
			return errors.Trace(err)
		}
		cnt++
		if cnt >= limit {
			break
		}
	}
	return nil
}

func (r *Reader) GetSnapshot() *engine.SnapAccess {
	return r.snap
}

func (r *Reader) Close() {
	if r.iter != nil {
		r.iter.Close()
	}
	if r.revIter != nil {
		r.revIter.Close()
	}
	if r.extraIter != nil {
		r.extraIter.Close()
	}
	r.snap.Discard()
}
