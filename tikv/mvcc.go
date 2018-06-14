package tikv

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coocood/badger"
	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/ngaut/faketikv/lockstore"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
)

// MVCCStore is a wrapper of badger.DB to provide MVCC functions.
type MVCCStore struct {
	dir             string
	db              *badger.DB
	writeDBWorker   *writeDBWorker
	lockStore       *lockstore.LockStore
	rollbackStore   *lockstore.LockStore
	writeLockWorker *writeLockWorker
	closeCh         chan struct{}
	wg              sync.WaitGroup

	// latestTS records the latest timestamp of requests, used to determine if it is safe to GC rollback key.
	latestTS uint64
}

// NewMVCCStore creates a new MVCCStore
func NewMVCCStore(db *badger.DB, dataDir string) *MVCCStore {
	ls := lockstore.NewLockStore(8 << 20)
	rollbackStore := lockstore.NewLockStore(256 << 10)
	closeCh := make(chan struct{})
	store := &MVCCStore{
		db:  db,
		dir: dataDir,
		writeDBWorker: &writeDBWorker{
			wakeUp:  make(chan struct{}, 1),
			closeCh: closeCh,
		},
		lockStore:     ls,
		rollbackStore: rollbackStore,
		writeLockWorker: &writeLockWorker{
			wakeUp:  make(chan struct{}, 1),
			closeCh: closeCh,
		},
		closeCh: closeCh,
	}
	store.writeDBWorker.store = store
	store.writeLockWorker.store = store
	err := store.loadLockStore()
	if err != nil {
		log.Fatal(err)
	}
	store.wg.Add(3)
	go store.writeDBWorker.run()
	go store.writeLockWorker.run()
	rbGCWorker := rollbackGCWorker{store: store}
	go rbGCWorker.run()
	return store
}

func (store *MVCCStore) Close() error {
	close(store.closeCh)
	store.wg.Wait()
	err := store.dumpLockStore()
	if err != nil {
		log.Error(err)
	}
	return nil
}

func (store *MVCCStore) getLatestTS() uint64 {
	return atomic.LoadUint64(&store.latestTS)
}

func (store *MVCCStore) updateLatestTS(ts uint64) {
	latestTS := store.getLatestTS()
	if ts != math.MaxUint64 && ts > latestTS {
		atomic.CompareAndSwapUint64(&store.latestTS, latestTS, ts)
	}
}

func (store *MVCCStore) Get(reqCtx *requestCtx, key []byte, startTS uint64) ([]byte, error) {
	store.updateLatestTS(startTS)
	g := store.newStoreGetter(reqCtx)
	defer g.Close()
	return g.get(key, startTS)
}

func (store *MVCCStore) newStoreGetter(reqCtx *requestCtx) *storeGetter {
	g := &storeGetter{ls: store.lockStore, reqCtx: reqCtx}
	g.txn = store.db.NewTransaction(false)
	return g
}

func newIterator(txn *badger.Txn) *badger.Iterator {
	var itOpts = badger.DefaultIteratorOptions
	itOpts.PrefetchValues = false
	return txn.NewIterator(itOpts)
}

type storeGetter struct {
	ls     *lockstore.LockStore
	txn    *badger.Txn
	reqCtx *requestCtx
	iter   *badger.Iterator
}

func (g *storeGetter) get(key []byte, startTS uint64) ([]byte, error) {
	g.reqCtx.buf = g.ls.Get(key, g.reqCtx.buf)
	if len(g.reqCtx.buf) > 0 {
		lock := decodeLock(g.reqCtx.buf)
		err := checkLock(lock, key, startTS)
		if err != nil {
			return nil, err
		}
	}
	item, err := g.txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, errors.Trace(err)
	}
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	mvVal, err := decodeValue(item)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if mvVal.commitTS <= startTS {
		return mvVal.value, nil
	}
	oldKey := encodeOldKey(key, startTS)
	if g.iter == nil {
		g.iter = newIterator(g.txn)
	}
	g.iter.Seek(oldKey)
	if !g.iter.ValidForPrefix(oldKey[:len(oldKey)-8]) {
		return nil, nil
	}
	item = g.iter.Item()
	mvVal, err = decodeValue(item)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return mvVal.value, nil
}

func (g *storeGetter) Close() {
	if g.iter != nil {
		g.iter.Close()
	}
	g.txn.Discard()
}

func checkLock(lock mvccLock, key []byte, startTS uint64) error {
	lockVisible := lock.startTS < startTS
	isWriteLock := lock.op == uint16(kvrpcpb.Op_Put) || lock.op == uint16(kvrpcpb.Op_Del)
	isPrimaryGet := lock.startTS == lockVer && bytes.Equal(lock.primary, key)
	if lockVisible && isWriteLock && !isPrimaryGet {
		return &ErrLocked{
			Key:     key,
			StartTS: lock.startTS,
			Primary: lock.primary,
			TTL:     uint64(lock.ttl),
		}
	}
	return nil
}

func (store *MVCCStore) BatchGet(reqCtx *requestCtx, keys [][]byte, startTS uint64) []Pair {
	store.updateLatestTS(startTS)
	g := store.newStoreGetter(reqCtx)
	defer g.Close()
	pairs := make([]Pair, 0, len(keys))
	for _, key := range keys {
		val, err := g.get(key, startTS)
		if len(val) == 0 {
			continue
		}
		pairs = append(pairs, Pair{Key: key, Value: val, Err: err})
	}
	return pairs
}

func (store *MVCCStore) Prewrite(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation, primary []byte, startTS uint64, ttl uint64) []error {
	store.updateLatestTS(startTS)
	regCtx := reqCtx.regCtx
	hashVals := mutationsToHashVals(mutations)
	errs := make([]error, 0, len(mutations))
	var anyError bool
	store.acquireLatches(regCtx, hashVals)
	defer regCtx.releaseLatches(hashVals)
	// Must check the LockStore first.
	for _, m := range mutations {
		err := store.checkPrewriteInLockStore(reqCtx, m, startTS)
		if err != nil {
			anyError = true
		}
		errs = append(errs, err)
	}
	if anyError {
		return errs
	}
	// Check the DB.
	err := store.db.View(func(txn *badger.Txn) error {
		for i, m := range mutations {
			err := store.checkPrewriteInDB(reqCtx, txn, m, startTS)
			if err != nil {
				anyError = true
			}
			errs[i] = err
		}
		return nil
	})
	if anyError {
		return errs
	}
	if err != nil {
		return []error{err}
	}

	batch := new(writeBatch)
	for _, m := range mutations {
		lock := mvccLock{
			mvccLockHdr: mvccLockHdr{
				startTS:    startTS,
				op:         uint16(m.Op),
				ttl:        uint32(ttl),
				primaryLen: uint16(len(primary)),
			},
			primary: primary,
			value:   m.Value,
		}
		batch.setWithMeta(m.Key, lock.MarshalBinary(), 0)
	}
	err = store.writeLock(batch)
	if err != nil {
		return []error{err}
	}
	return nil
}

func (store *MVCCStore) checkPrewriteInLockStore(req *requestCtx, mutation *kvrpcpb.Mutation, startTS uint64) error {
	req.buf = encodeRollbackKey(req.buf, mutation.Key, startTS)
	if len(store.rollbackStore.Get(req.buf, nil)) > 0 {
		return ErrAbort("already rollback")
	}
	req.buf = store.lockStore.Get(mutation.Key, req.buf)
	if len(req.buf) == 0 {
		return nil
	}
	lock := decodeLock(req.buf)
	if lock.startTS == startTS {
		// Same ts, no need to overwrite.
		return nil
	}
	return &ErrLocked{
		Key:     mutation.Key,
		StartTS: lock.startTS,
		Primary: lock.primary,
		TTL:     uint64(lock.ttl),
	}
}

func (store *MVCCStore) checkPrewriteInDB(req *requestCtx, txn *badger.Txn, mutation *kvrpcpb.Mutation, startTS uint64) error {
	item, err := txn.Get(mutation.Key)
	if err != nil && err != badger.ErrKeyNotFound {
		return errors.Trace(err)
	}
	if item == nil {
		return nil
	}
	mvVal, err := decodeValue(item)
	if err != nil {
		return errors.Trace(err)
	}
	if mvVal.commitTS > startTS {
		return ErrRetryable("write conflict")
	}
	return nil
}

const lockVer uint64 = math.MaxUint64

// Commit implements the MVCCStore interface.
func (store *MVCCStore) Commit(req *requestCtx, keys [][]byte, startTS, commitTS uint64) error {
	store.updateLatestTS(commitTS)
	regCtx := req.regCtx
	hashVals := keysToHashVals(keys)
	store.acquireLatches(regCtx, hashVals)
	defer regCtx.releaseLatches(hashVals)
	commitBatch := new(writeBatch)
	var buf []byte
	var tmpDiff int
	for _, key := range keys {
		buf = store.lockStore.Get(key, buf)
		if len(buf) == 0 {
			// We never commit partial keys in Commit request, so if one lock is not found,
			// the others keys must not be found too.
			return store.handleLockNotFound(key, startTS, commitTS)
		}
		lock := decodeLock(buf)
		if lock.startTS != startTS {
			return errors.New("replaced by another transaction")
		}
		if lock.op == uint16(kvrpcpb.Op_Lock) {
			continue
		}
		mvVal, userMeta := mvLockToMvVal(lock, commitTS)
		commitVal := mvVal.MarshalBinary()
		tmpDiff += len(key) + len(commitVal)
		commitBatch.setWithMeta(key, commitVal, userMeta)
	}
	// Move current latest to old.
	err := store.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			item, err := txn.Get(key)
			if err != nil && err != badger.ErrKeyNotFound {
				return errors.Trace(err)
			}
			if item == nil {
				continue
			}
			mvVal, err := decodeValue(item)
			if err != nil {
				return errors.Trace(err)
			}
			oldKey := encodeOldKey(key, mvVal.commitTS)
			var userMeta byte
			if len(mvVal.value) == 0 {
				userMeta = userMetaDelete
			} else {
				userMeta = 0
			}
			commitBatch.setWithMeta(oldKey, mvVal.MarshalBinary(), userMeta)
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	atomic.AddInt64(&regCtx.diff, int64(tmpDiff))
	err = store.write(commitBatch)
	if err != nil {
		return errors.Trace(err)
	}
	// We must delete lock after commit succeed, or there will be inconsistency.
	delLockBatch := new(writeBatch)
	for _, key := range keys {
		delLockBatch.delete(key)
	}
	err = store.writeLock(delLockBatch)
	return errors.Trace(err)
}

func (store *MVCCStore) handleLockNotFound(key []byte, startTS, commitTS uint64) error {
	err := store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return errors.Trace(err)
		}
		mvVal, err := decodeValue(item)
		if err != nil {
			return errors.Trace(err)
		}
		if mvVal.startTS == startTS {
			// Already committed.
			return nil
		} else {
			// The transaction may be committed and moved to old data, we need to look for that.
			oldKey := encodeOldKey(key, commitTS)
			_, err = txn.Get(oldKey)
			if err == nil {
				// Found committed key.
				return nil
			}
		}
		return errors.New("lock not found")
	})
	return errors.Trace(err)
}

func (store *MVCCStore) Rollback(reqCtx *requestCtx, keys [][]byte, startTS uint64) error {
	store.updateLatestTS(startTS)
	hashVals := keysToHashVals(keys)
	store.acquireLatches(reqCtx.regCtx, hashVals)
	defer reqCtx.regCtx.releaseLatches(hashVals)
	wb := new(writeBatch)
	err1 := store.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			err := store.rollbackKey(wb, txn, key, startTS)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err1 != nil {
		log.Error(err1)
		return err1
	}
	return store.writeLock(wb)
}

func (store *MVCCStore) rollbackKey(batch *writeBatch, txn *badger.Txn, key []byte, startTS uint64) error {
	batch.buf = encodeRollbackKey(batch.buf, key, startTS)
	rollbackKey := safeCopy(batch.buf)
	batch.buf = store.rollbackStore.Get(rollbackKey, batch.buf)
	if len(batch.buf) != 0 {
		// Already rollback.
		return nil
	}
	batch.buf = store.lockStore.Get(key, batch.buf)
	hasLock := len(batch.buf) > 0
	if hasLock {
		lock := decodeLock(batch.buf)
		if lock.startTS < startTS {
			// The lock is old, means this is written by an old transaction, and the current transaction may not arrive.
			// We should write a rollback lock.
			batch.rollback(rollbackKey)
			return nil
		}
		if lock.startTS == startTS {
			// We can not simply delete the lock because the prewrite may be sent multiple times.
			// To prevent that we update it a rollback lock.
			batch.rollback(rollbackKey)
			batch.delete(key)
			return nil
		}
		// lock.startTS > startTS, go to DB to check if the key is committed.
	}
	item, err := txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return errors.Trace(err)
	}
	hasVal := item != nil
	if !hasLock && !hasVal {
		// The prewrite request is not arrived, we write a rollback lock to prevent the future prewrite.
		batch.rollback(rollbackKey)
		return nil
	}

	if !hasVal {
		// Not committed.
		return nil
	}
	val, err := decodeValue(item)
	if err != nil {
		return errors.Trace(err)
	}
	if val.startTS == startTS {
		return ErrAlreadyCommitted(val.commitTS)
	}
	if val.startTS < startTS && !hasLock {
		// Prewrite and commit have not arrived.
		batch.rollback(rollbackKey)
		return nil
	}
	// val.startTS > startTS, look for the key in the old version to check if the key is committed.
	iter := newIterator(txn)
	defer iter.Close()
	oldKey := encodeOldKey(key, val.commitTS)
	// find greater commit version.
	for iter.Seek(oldKey); iter.ValidForPrefix(oldKey[:len(oldKey)-8]); iter.Next() {
		item := iter.Item()
		foundKey := item.Key()
		if isVisibleKey(foundKey, startTS) {
			break
		}
		_, ts, err := codec.DecodeUintDesc(foundKey[len(foundKey)-8:])
		if err != nil {
			return errors.Trace(err)
		}
		mvVal, err := decodeValue(item)
		if mvVal.startTS == startTS {
			return ErrAlreadyCommitted(ts)
		}
	}
	return nil
}

func (store *MVCCStore) checkRangeLock(regCtx *regionCtx, startKey, endKey []byte, startTS uint64) error {
	lockIter := store.lockStore.NewIterator()
	for lockIter.Seek(startKey); lockIter.Valid(); lockIter.Next() {
		if exceedEndKey(lockIter.Key(), endKey) {
			break
		}
		lock := decodeLock(lockIter.Value())
		err := checkLock(lock, lockIter.Key(), startTS)
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *MVCCStore) Scan(reqCtx *requestCtx, startKey, endKey []byte, limit int, startTS uint64, ignoreLock bool) []Pair {
	store.updateLatestTS(startTS)
	if !ignoreLock {
		err := store.checkRangeLock(reqCtx.regCtx, startKey, endKey, startTS)
		if err != nil {
			return []Pair{{Err: err}}
		}
	}
	var pairs []Pair
	err := store.db.View(func(txn *badger.Txn) error {
		iter := newIterator(txn)
		defer iter.Close()
		var oldIter *badger.Iterator
		for iter.Seek(startKey); iter.Valid(); iter.Next() {
			item := iter.Item()
			if exceedEndKey(item.Key(), endKey) {
				return nil
			}
			key := item.KeyCopy(nil)
			mvVal, err := decodeValue(item)
			if err != nil {
				return errors.Trace(err)
			}
			if mvVal.commitTS > startTS {
				if oldIter == nil {
					oldIter = newIterator(txn)
				}
				mvVal, err = store.getOldValue(oldIter, encodeOldKey(item.Key(), startTS))
				if err == badger.ErrKeyNotFound {
					continue
				}
			}
			if len(mvVal.value) == 0 {
				continue
			}
			pairs = append(pairs, Pair{Key: key, Value: mvVal.value})
			if len(pairs) >= limit {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return []Pair{{Err: err}}
	}
	return pairs
}

func (store *MVCCStore) getOldValue(oldIter *badger.Iterator, oldKey []byte) (mvccValue, error) {
	oldIter.Seek(oldKey)
	if !oldIter.ValidForPrefix(oldKey[:len(oldKey)-8]) {
		return mvccValue{}, badger.ErrKeyNotFound
	}
	return decodeValue(oldIter.Item())
}

func isVisibleKey(key []byte, startTS uint64) bool {
	ts := ^(binary.BigEndian.Uint64(key[len(key)-8:]))
	return startTS >= ts
}

// ReverseScan implements the MVCCStore interface. The search range is [startKey, endKey).
func (store *MVCCStore) ReverseScan(reqCtx *requestCtx, startKey, endKey []byte, limit int, startTS uint64, ignoreLock bool) []Pair {
	store.updateLatestTS(startTS)
	if !ignoreLock {
		err := store.checkRangeLock(reqCtx.regCtx, startKey, endKey, startTS)
		if err != nil {
			return []Pair{{Err: err}}
		}
	}
	var pairs []Pair
	err := store.db.View(func(txn *badger.Txn) error {
		var opts badger.IteratorOptions
		opts.Reverse = true
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()
		var oldIter *badger.Iterator
		for iter.Seek(endKey); iter.Valid(); iter.Next() {
			item := iter.Item()
			if bytes.Compare(item.Key(), startKey) < 0 {
				return nil
			}
			key := item.KeyCopy(nil)
			mvVal, err := decodeValue(item)
			if err != nil {
				return errors.Trace(err)
			}
			if mvVal.commitTS > startTS {
				if oldIter == nil {
					oldIter = newIterator(txn)
				}
				mvVal, err = store.getOldValue(oldIter, encodeOldKey(item.Key(), startTS))
				if err == badger.ErrKeyNotFound {
					continue
				}
			}
			if len(mvVal.value) == 0 {
				continue
			}
			pairs = append(pairs, Pair{Key: key, Value: mvVal.value})
			if len(pairs) >= limit {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return []Pair{{Err: err}}
	}
	return pairs
}

func (store *MVCCStore) Cleanup(reqCtx *requestCtx, key []byte, startTS uint64) error {
	store.updateLatestTS(startTS)
	hashVals := keysToHashVals([][]byte{key})
	store.acquireLatches(reqCtx.regCtx, hashVals)
	defer reqCtx.regCtx.releaseLatches(hashVals)
	wb := new(writeBatch)
	err := store.db.View(func(txn *badger.Txn) error {
		return store.rollbackKey(wb, txn, key, startTS)
	})
	if err != nil {
		return err
	}
	store.write(wb)
	return err
}

func (store *MVCCStore) ScanLock(reqCtx *requestCtx, maxTS uint64) ([]*kvrpcpb.LockInfo, error) {
	var locks []*kvrpcpb.LockInfo
	it := store.lockStore.NewIterator()
	for it.Seek(reqCtx.regCtx.startKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), reqCtx.regCtx.endKey) {
			return locks, nil
		}
		lock := decodeLock(it.Value())
		if lock.startTS < maxTS {
			locks = append(locks, &kvrpcpb.LockInfo{
				PrimaryLock: lock.primary,
				LockVersion: lock.startTS,
				Key:         codec.EncodeBytes(nil, it.Key()),
				LockTtl:     uint64(lock.ttl),
			})
		}
	}
	return locks, nil
}

func (store *MVCCStore) ResolveLock(reqCtx *requestCtx, startTS, commitTS uint64) error {
	regCtx := reqCtx.regCtx
	var lockKeys [][]byte
	var lockVals [][]byte
	it := store.lockStore.NewIterator()
	for it.Seek(regCtx.startKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), regCtx.endKey) {
			break
		}
		lock := decodeLock(it.Value())
		if lock.startTS != startTS {
			continue
		}
		lockKeys = append(lockKeys, safeCopy(it.Key()))
		lockVals = append(lockVals, safeCopy(it.Value()))
	}
	if len(lockKeys) == 0 {
		return nil
	}
	hashVals := keysToHashVals(lockKeys)
	store.acquireLatches(regCtx, hashVals)
	defer regCtx.releaseLatches(hashVals)
	lockBatch := new(writeBatch)
	var buf []byte
	var commitBatch *writeBatch
	if commitTS > 0 {
		commitBatch = new(writeBatch)
	}
	for i, lockKey := range lockKeys {
		buf = store.lockStore.Get(lockKey, buf)
		// We need to check again make sure the lock is not changed.
		if bytes.Equal(buf, lockVals[i]) {
			if commitTS > 0 {
				lock := decodeLock(lockVals[i])
				mvVal, useMeta := mvLockToMvVal(lock, commitTS)
				commitBatch.setWithMeta(lockKey, mvVal.MarshalBinary(), useMeta)
			}
			lockBatch.delete(lockKey)
		}
	}
	if len(lockBatch.entries) == 0 {
		return nil
	}
	if commitBatch != nil {
		atomic.AddInt64(&regCtx.diff, commitBatch.size())
		err := store.write(commitBatch)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return store.writeLock(lockBatch)
}

const delRangeBatchSize = 4096

func (store *MVCCStore) DeleteRange(reqCtx *requestCtx, startKey, endKey []byte) error {
	keys := make([][]byte, 0, delRangeBatchSize)
	oldStartKey := encodeOldKey(startKey, lockVer)
	oldEndKey := encodeOldKey(endKey, lockVer)

	err := store.db.View(func(txn *badger.Txn) error {
		iter := newIterator(txn)
		defer iter.Close()
		keys = store.collectRangeKeys(iter, startKey, endKey, keys)
		keys = store.collectRangeKeys(iter, oldStartKey, oldEndKey, keys)
		return nil
	})
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}
	err = store.deleteKeysInBatch(reqCtx.regCtx, keys, delRangeBatchSize)
	if err != nil {
		log.Error(err)
	}
	return errors.Trace(err)
}

func (store *MVCCStore) collectRangeKeys(iter *badger.Iterator, startKey, endKey []byte, keys [][]byte) [][]byte {
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		if exceedEndKey(key, endKey) {
			break
		}
		keys = append(keys, key)
		if len(keys) == delRangeBatchSize {
			break
		}
	}
	return keys
}

func (store *MVCCStore) deleteKeysInBatch(regCtx *regionCtx, keys [][]byte, batchSize int) error {
	for len(keys) > 0 {
		batchSize := mathutil.Min(len(keys), batchSize)
		batchKeys := keys[:batchSize]
		keys = keys[batchSize:]

		hashVals := keysToHashVals(batchKeys)
		store.acquireLatches(regCtx, hashVals)
		wb := new(writeBatch)
		err := store.db.View(func(txn *badger.Txn) error {
			for _, key := range batchKeys {
				wb.delete(key)
			}
			return nil
		})
		if err != nil {
			log.Error(err)
			regCtx.releaseLatches(hashVals)
			return errors.Trace(err)
		}
		err = store.write(wb)
		regCtx.releaseLatches(hashVals)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (store *MVCCStore) GC(reqCtx *requestCtx, safePoint uint64) error {
	// TODO: implement GC in badger.
	return nil
}

func (store *MVCCStore) acquireLatches(regCtx *regionCtx, hashVals []uint64) {
	start := time.Now()
	for {
		ok, wg, lockLen := regCtx.acquireLatches(hashVals)
		if ok {
			dur := time.Since(start)
			if dur > time.Millisecond*50 {
				log.Warnf("acquire %d locks takes %v, memLock size %d", len(hashVals), dur, lockLen)
			}
			return
		}
		wg.Wait()
	}
}
