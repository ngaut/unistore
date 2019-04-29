package tikv

import (
	"bytes"
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coocood/badger"
	"github.com/coocood/badger/y"
	"github.com/cznic/mathutil"
	"github.com/dgryski/go-farm"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/rowcodec"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/ngaut/unistore/util/deadlock"
	"github.com/ngaut/unistore/util/lockwaiter"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
)

// MVCCStore is a wrapper of badger.DB to provide MVCC functions.
type MVCCStore struct {
	dir             string
	db              *badger.DB
	writeDBWorker   *writeDBWorker
	lockStore       *lockstore.MemStore
	rollbackStore   *lockstore.MemStore
	writeLockWorker *writeLockWorker
	closeCh         chan struct{}
	wg              sync.WaitGroup

	// latestTS records the latest timestamp of requests, used to determine if it is safe to GC rollback key.
	latestTS uint64

	safePoint *SafePoint
	gcLock    sync.Mutex

	deadlockDetector  *deadlock.Detector
	lockWaiterManager *lockwaiter.Manager
}

// NewMVCCStore creates a new MVCCStore
func NewMVCCStore(db *badger.DB, dataDir string, safePoint *SafePoint) *MVCCStore {
	ls := lockstore.NewMemStore(8 << 20)
	rollbackStore := lockstore.NewMemStore(256 << 10)
	closeCh := make(chan struct{})
	store := &MVCCStore{
		db:                db,
		dir:               dataDir,
		lockStore:         ls,
		rollbackStore:     rollbackStore,
		closeCh:           closeCh,
		safePoint:         safePoint,
		deadlockDetector:  deadlock.NewDetector(),
		lockWaiterManager: lockwaiter.NewManager(),
	}
	store.writeLockWorker = &writeLockWorker{
		batchCh: make(chan *writeLockBatch, batchChanSize),
		closeCh: closeCh,
		store:   store,
	}
	store.writeDBWorker = &writeDBWorker{
		batchCh: make(chan *writeDBBatch, batchChanSize),
		closeCh: closeCh,
		store:   store,
	}
	err := store.loadLocks()
	if err != nil {
		log.Fatal(err)
	}
	store.loadSafePoint()

	// mark worker count
	store.wg.Add(3)
	go store.writeDBWorker.run()
	go store.writeLockWorker.run()
	go func() {
		rbGCWorker := rollbackGCWorker{store: store}
		rbGCWorker.run()
	}()

	return store
}

func (store *MVCCStore) loadSafePoint() {
	err := store.db.View(func(txn *badger.Txn) error {
		item, err1 := txn.Get(InternalSafePointKey)
		if err1 == badger.ErrKeyNotFound {
			return nil
		} else if err1 != nil {
			return err1
		}
		val, err1 := item.Value()
		if err1 != nil {
			return err1
		}
		atomic.StoreUint64(&store.safePoint.timestamp, binary.LittleEndian.Uint64(val))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}

func (store *MVCCStore) Close() error {
	close(store.closeCh)
	store.wg.Wait()

	err := store.dumpMemLocks()
	if err != nil {
		log.Fatal(err)
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

func (store *MVCCStore) PessimisticLock(reqCtx *requestCtx, req *kvrpcpb.PessimisticLockRequest) (*lockwaiter.Waiter, error) {
	mutations := deduplicateMutationss(req.Mutations)
	startTS := req.StartVersion
	forUpdateTS := req.ForUpdateTs
	primary := req.PrimaryLock
	ttl := req.LockTtl
	regCtx := reqCtx.regCtx
	hashVals := mutationsToHashVals(mutations)
	regCtx.acquireLatches(hashVals)
	defer regCtx.releaseLatches(hashVals)

	// check again after acquired latch.
	dup, err := store.checkPessimisticLock(reqCtx, mutations, startTS)
	if dup || err != nil {
		return store.handleCheckPessimisticErr(startTS, err, req.IsFirstLock)
	}

	lockBatch := newWriteLockBatch(reqCtx)
	// Check the DB.
	txn := reqCtx.getDBReader().GetTxn()
	keys := make([][]byte, len(mutations))
	for i, m := range mutations {
		keys[i] = m.Key
	}
	items, err := txn.MultiGet(keys)
	if err != nil {
		return nil, err
	}

	for i, m := range mutations {
		// Note the use of `forUpdateTS` here!
		oldMeta, oldVal, err := store.checkConflictInDB(reqCtx, txn, items[i], forUpdateTS)
		if err != nil {
			return nil, err
		}
		if len(oldVal) > 0 && m.Assertion == kvrpcpb.Assertion_NotExist {
			return nil, &ErrKeyAlreadyExists{Key: m.Key}
		}
		lock := mvcc.MvccLock{
			MvccLockHdr: mvcc.MvccLockHdr{
				StartTS:    startTS,
				Op:         uint8(kvrpcpb.Op_PessimisticLock),
				HasOldVer:  oldMeta != nil,
				TTL:        uint32(ttl),
				PrimaryLen: uint16(len(primary)),
			},
			Primary: primary,
			Value:   m.Value,
			OldMeta: oldMeta,
			OldVal:  oldVal,
		}
		lockBatch.set(m.Key, lock.MarshalBinary())
	}
	err = store.writeLocks(lockBatch)
	log.Info(startTS, "locked", hashVals)
	return nil, err
}

func (store *MVCCStore) handleCheckPessimisticErr(startTS uint64, err error, isFirstLock bool) (*lockwaiter.Waiter, error) {
	if lock, ok := err.(*ErrLocked); ok {
		keyHash := farm.Fingerprint64(lock.Key)
		if !isFirstLock { // No need to detect deadlock if it's first lock.
			if err1 := store.deadlockDetector.Detect(startTS, lock.StartTS, keyHash); err1 != nil {
				log.Errorf("%d deadlock for %d on key %d", startTS, lock.StartTS, keyHash)
				return nil, err1
			}
		}
		log.Infof("%d blocked by %d on key %d", startTS, lock.StartTS, keyHash)
		waiter := store.lockWaiterManager.NewWaiter(startTS, lock.StartTS, farm.Fingerprint64(lock.Key), time.Second)
		return waiter, err
	}
	return nil, err
}

func (store *MVCCStore) checkPessimisticLock(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation, startTS uint64) (dup bool, err error) {
	for _, m := range mutations {
		lock, err := store.checkConflictInLockStore(reqCtx, m, startTS)
		if err != nil {
			return false, err
		}
		if lock != nil {
			// The lock already exists, it's a duplicate command, we can return directly.
			y.Assert(lock.Op == uint8(kvrpcpb.Op_PessimisticLock))
			return true, nil
		}
	}
	return false, nil
}

func (store *MVCCStore) Prewrite(reqCtx *requestCtx, req *kvrpcpb.PrewriteRequest) []error {
	mutations := deduplicateMutationss(req.Mutations)
	startTS := req.StartVersion
	primary := req.PrimaryLock
	ttl := req.LockTtl
	regCtx := reqCtx.regCtx
	hashVals := mutationsToHashVals(mutations)
	errs := make([]error, 0, len(mutations))
	anyError := false

	regCtx.acquireLatches(hashVals)
	defer regCtx.releaseLatches(hashVals)
	lockBatch := newWriteLockBatch(reqCtx)
	hasPessimistic := len(req.IsPessimisticLock) > 0
	// Must check the LockStore first.
	for i, m := range mutations {
		lock, err := store.checkConflictInLockStore(reqCtx, m, req.StartVersion)
		if err != nil {
			anyError = true
		}
		if hasPessimistic && req.IsPessimisticLock[i] {
			if lock != nil && lock.Op == uint8(kvrpcpb.Op_PessimisticLock) {
				// lockstore doesn't support update for now, delete the lock first.
				lockBatch.delete(m.Key)
			} else {
				log.Error(startTS, "pessimistic lock not exists", farm.Fingerprint64(m.Key))
				return []error{errors.New("pessimistic lock not exists")}
			}
		} else if lock != nil {
			// duplicated command.
			return nil
		}
		errs = append(errs, err)
	}
	if anyError {
		return errs
	}

	// Check the DB.
	txn := reqCtx.getDBReader().GetTxn()
	keys := make([][]byte, len(mutations))
	for i, m := range mutations {
		keys[i] = m.Key
	}
	items, err := txn.MultiGet(keys)
	if err != nil {
		return []error{err}
	}
	var buf []byte
	var enc rowcodec.Encoder
	checkConflictTS := startTS
	if hasPessimistic {
		checkConflictTS = math.MaxUint64
	}
	for i, m := range mutations {
		oldMeta, oldVal, err := store.checkConflictInDB(reqCtx, txn, items[i], checkConflictTS)
		if err != nil {
			anyError = true
		}
		errs[i] = err
		if !anyError {
			op := m.Op
			if op == kvrpcpb.Op_Insert {
				if len(oldVal) > 0 {
					return []error{&ErrKeyAlreadyExists{Key: m.Key}}
				}
				op = kvrpcpb.Op_Put
			}
			if rowcodec.IsRowKey(m.Key) && op == kvrpcpb.Op_Put {
				buf, err = enc.EncodeFromOldRow(m.Value, buf)
				if err != nil {
					log.Errorf("err:%v m.Value:%v m.Key:%q m.Op:%d", err, m.Value, m.Key, m.Op)
					return []error{err}
				}
				m.Value = buf
			}
			lock := mvcc.MvccLock{
				MvccLockHdr: mvcc.MvccLockHdr{
					StartTS:    startTS,
					Op:         uint8(op),
					HasOldVer:  oldMeta != nil,
					TTL:        uint32(ttl),
					PrimaryLen: uint16(len(primary)),
				},
				Primary: primary,
				Value:   m.Value,
				OldMeta: oldMeta,
				OldVal:  oldVal,
			}
			lockBatch.set(m.Key, lock.MarshalBinary())
		}
	}
	if anyError {
		return errs
	}
	err = store.writeLocks(lockBatch)
	if err != nil {
		return []error{err}
	}
	return nil
}

func (store *MVCCStore) checkConflictInLockStore(
	req *requestCtx, mutation *kvrpcpb.Mutation, startTS uint64) (*mvcc.MvccLock, error) {
	req.buf = mvcc.EncodeRollbackKey(req.buf, mutation.Key, startTS)
	if len(store.rollbackStore.Get(req.buf, nil)) > 0 {
		return nil, ErrAlreadyRollback
	}
	req.buf = store.lockStore.Get(mutation.Key, req.buf)
	if len(req.buf) == 0 {
		return nil, nil
	}
	lock := mvcc.DecodeLock(req.buf)
	if lock.StartTS == startTS {
		// Same ts, no need to overwrite.
		return &lock, nil
	}
	return nil, &ErrLocked{
		Key:     mutation.Key,
		StartTS: lock.StartTS,
		Primary: lock.Primary,
		TTL:     uint64(lock.TTL),
	}
}

// checkPrewrietInDB checks that there is no committed version greater than startTS or return write conflict error.
// And it returns the old version if there is one.
func (store *MVCCStore) checkConflictInDB(
	req *requestCtx, txn *badger.Txn, item *badger.Item, startTS uint64) (userMeta mvcc.DBUserMeta, val []byte, err error) {
	if item == nil {
		return nil, nil, nil
	}
	userMeta = mvcc.DBUserMeta(item.UserMeta())
	if userMeta.CommitTS() > startTS {
		return nil, nil, &ErrConflict{
			StartTS:    startTS,
			ConflictTS: userMeta.CommitTS(),
			Key:        item.KeyCopy(nil),
		}
	}
	val, err = item.Value()
	if err != nil {
		return nil, nil, err
	}
	return userMeta, val, nil
}

const maxSystemTS uint64 = math.MaxUint64

// Commit implements the MVCCStore interface.
func (store *MVCCStore) Commit(req *requestCtx, keys [][]byte, startTS, commitTS uint64) error {
	store.updateLatestTS(commitTS)
	regCtx := req.regCtx
	hashVals := keysToHashVals(keys...)
	dbBatch := newWriteDBBatch(req)
	userMeta := mvcc.NewDBUserMeta(startTS, commitTS)

	regCtx.acquireLatches(hashVals)
	defer regCtx.releaseLatches(hashVals)

	var buf []byte
	var tmpDiff int
	var hasPessimisticLock bool
	for _, key := range keys {
		buf = store.lockStore.Get(key, buf)
		if len(buf) == 0 {
			// We never commit partial keys in Commit request, so if one lock is not found,
			// the others keys must not be found too.
			return store.handleLockNotFound(req, key, startTS, commitTS)
		}
		lock := mvcc.DecodeLock(buf)
		if lock.StartTS != startTS {
			return ErrReplaced
		}
		if lock.Op == uint8(kvrpcpb.Op_PessimisticLock) {
			hasPessimisticLock = true
		}
		if lock.Op == uint8(kvrpcpb.Op_Lock) {
			continue
		}
		tmpDiff += len(key) + len(lock.Value) + len(userMeta)
		dbBatch.set(key, lock.Value, userMeta)
		if lock.HasOldVer {
			oldKey := mvcc.EncodeOldKey(key, lock.OldMeta.CommitTS())
			dbBatch.set(oldKey, lock.OldVal, lock.OldMeta.ToOldUserMeta(commitTS))
		}
	}
	if hasPessimisticLock {
		store.deadlockDetector.CleanUp(startTS)
	}
	atomic.AddInt64(&regCtx.diff, int64(tmpDiff))
	err := store.writeDB(dbBatch)
	if err != nil {
		return errors.Trace(err)
	}
	// We must delete lock after commit succeed, or there will be inconsistency.
	lockBatch := newWriteLockBatch(req)
	for _, key := range keys {
		lockBatch.delete(key)
	}
	err = store.writeLocks(lockBatch)
	store.lockWaiterManager.WakeUp(startTS, commitTS, hashVals)
	return errors.Trace(err)
}

func (store *MVCCStore) handleLockNotFound(reqCtx *requestCtx, key []byte, startTS, commitTS uint64) error {
	txn := reqCtx.getDBReader().GetTxn()
	item, err := txn.Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return errors.Trace(err)
	}
	if item == nil {
		return ErrLockNotFound
	}
	useMeta := mvcc.DBUserMeta(item.UserMeta())
	if useMeta.StartTS() == startTS {
		// Already committed.
		return nil
	} else {
		// The transaction may be committed and moved to old data, we need to look for that.
		oldKey := mvcc.EncodeOldKey(key, commitTS)
		_, err = txn.Get(oldKey)
		if err == nil {
			// Found committed key.
			return nil
		}
	}
	return ErrLockNotFound
}

const (
	rollbackStatusDone    = 0
	rollbackStatusNoLock  = 1
	rollbackStatusNewLock = 2
	rollbackPessimistic   = 3
)

func (store *MVCCStore) Rollback(reqCtx *requestCtx, keys [][]byte, startTS uint64) error {
	store.updateLatestTS(startTS)
	keys = deduplicateKeys(keys)
	hashVals := keysToHashVals(keys...)
	log.Warnf("%d rollback %v", startTS, hashVals)
	regCtx := reqCtx.regCtx
	lockBatch := newWriteLockBatch(reqCtx)

	regCtx.acquireLatches(hashVals)
	defer regCtx.releaseLatches(hashVals)

	statuses := make([]int, len(keys))
	var isPessimistic bool
	for i, key := range keys {
		statuses[i] = store.rollbackKeyReadLock(lockBatch, key, startTS)
		if statuses[i] == rollbackPessimistic {
			isPessimistic = true
		}
	}
	// rollback pessimistic lock doesn't need to check DB.
	if !isPessimistic {
		for i, key := range keys {
			if statuses[i] == rollbackStatusDone {
				continue
			}
			err := store.rollbackKeyReadDB(reqCtx, lockBatch, key, startTS, statuses[i] == rollbackStatusNewLock)
			if err != nil {
				return err
			}
		}
	}
	err := store.writeLocks(lockBatch)
	store.lockWaiterManager.WakeUp(startTS, 0, hashVals)
	return errors.Trace(err)
}

func deduplicateKeys(keys [][]byte) [][]byte {
	m := make(map[string]struct{})
	deduped := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if _, ok := m[string(key)]; !ok {
			m[string(key)] = struct{}{}
			deduped = append(deduped, key)
		}
	}
	return deduped
}

func deduplicateMutationss(mutations []*kvrpcpb.Mutation) []*kvrpcpb.Mutation {
	m := make(map[string]struct{})
	deduped := make([]*kvrpcpb.Mutation, 0, len(mutations))
	for _, mu := range mutations {
		if _, ok := m[string(mu.Key)]; !ok {
			m[string(mu.Key)] = struct{}{}
			deduped = append(deduped, mu)
		}
	}
	return deduped
}

func (store *MVCCStore) rollbackKeyReadLock(batch *writeLockBatch, key []byte, startTS uint64) (status int) {
	batch.buf = mvcc.EncodeRollbackKey(batch.buf, key, startTS)
	rollbackKey := safeCopy(batch.buf)
	batch.buf = store.rollbackStore.Get(rollbackKey, batch.buf)
	if len(batch.buf) != 0 {
		// Already rollback.
		return rollbackStatusDone
	}
	batch.buf = store.lockStore.Get(key, batch.buf)
	hasLock := len(batch.buf) > 0
	if hasLock {
		lock := mvcc.DecodeLock(batch.buf)
		if lock.StartTS < startTS {
			// The lock is old, means this is written by an old transaction, and the current transaction may not arrive.
			// We should write a rollback lock.
			batch.rollback(rollbackKey)
			return rollbackStatusDone
		}
		if lock.StartTS == startTS {
			// We can not simply delete the lock because the prewrite may be sent multiple times.
			// To prevent that we update it a rollback lock.
			batch.rollback(rollbackKey)
			batch.delete(key)
			if lock.Op == uint8(kvrpcpb.Op_PessimisticLock) {
				store.deadlockDetector.CleanUp(startTS)
				return rollbackPessimistic
			}
			return rollbackStatusDone
		}
		// lock.startTS > startTS, go to DB to check if the key is committed.
		return rollbackStatusNewLock
	}
	return rollbackStatusNoLock
}

func (store *MVCCStore) rollbackKeyReadDB(req *requestCtx, batch *writeLockBatch, key []byte, startTS uint64, hasLock bool) error {
	batch.buf = mvcc.EncodeRollbackKey(batch.buf, key, startTS)
	rollbackKey := safeCopy(batch.buf)
	reader := req.getDBReader()
	item, err := reader.GetTxn().Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return errors.Trace(err)
	}
	hasVal := item != nil
	if !hasVal && !hasLock {
		// The prewrite request is not arrived, we write a rollback lock to prevent the future prewrite.
		batch.rollback(rollbackKey)
		return nil
	}

	if !hasVal {
		// Not committed.
		return nil
	}
	userMeta := mvcc.DBUserMeta(item.UserMeta())
	if userMeta.StartTS() == startTS {
		return ErrAlreadyCommitted(userMeta.CommitTS())
	}
	if userMeta.StartTS() < startTS && !hasLock {
		// Prewrite and commit have not arrived.
		batch.rollback(rollbackKey)
		return nil
	}
	// val.startTS > startTS, look for the key in the old version to check if the key is committed.
	it := reader.GetOldIter()
	oldKey := mvcc.EncodeOldKey(key, userMeta.CommitTS())
	// find greater commit version.
	for it.Seek(oldKey); it.ValidForPrefix(oldKey[:len(oldKey)-8]); it.Next() {
		item := it.Item()
		foundKey := item.Key()
		if isVisibleKey(foundKey, startTS) {
			break
		}
		_, ts, err := codec.DecodeUintDesc(foundKey[len(foundKey)-8:])
		if err != nil {
			return errors.Trace(err)
		}
		if mvcc.OldUserMeta(item.UserMeta()).StartTS() == startTS {
			return ErrAlreadyCommitted(ts)
		}
	}
	return nil
}

func isVisibleKey(key []byte, startTS uint64) bool {
	ts := ^(binary.BigEndian.Uint64(key[len(key)-8:]))
	return startTS >= ts
}

func checkLock(lock mvcc.MvccLock, key []byte, startTS uint64) error {
	lockVisible := lock.StartTS < startTS
	isWriteLock := lock.Op == uint8(kvrpcpb.Op_Put) || lock.Op == uint8(kvrpcpb.Op_Del)
	isPrimaryGet := startTS == maxSystemTS && bytes.Equal(lock.Primary, key)
	if lockVisible && isWriteLock && !isPrimaryGet {
		return &ErrLocked{
			Key:     key,
			StartTS: lock.StartTS,
			Primary: lock.Primary,
			TTL:     uint64(lock.TTL),
		}
	}
	return nil
}

func (store *MVCCStore) CheckKeysLock(startTS uint64, keys ...[]byte) error {
	var buf []byte
	for _, key := range keys {
		buf = store.lockStore.Get(key, buf)
		if len(buf) == 0 {
			continue
		}
		lock := mvcc.DecodeLock(buf)
		err := checkLock(lock, key, startTS)
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *MVCCStore) CheckRangeLock(startTS uint64, startKey, endKey []byte) error {
	if len(endKey) == 0 {
		panic("invalid end key")
	}

	it := store.lockStore.NewIterator()
	for it.Seek(startKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), endKey) {
			break
		}
		lock := mvcc.DecodeLock(it.Value())
		err := checkLock(lock, it.Key(), startTS)
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *MVCCStore) Cleanup(reqCtx *requestCtx, key []byte, startTS uint64) error {
	store.updateLatestTS(startTS)
	hashVals := keysToHashVals(key)
	regCtx := reqCtx.regCtx
	lockBatch := newWriteLockBatch(reqCtx)

	regCtx.acquireLatches(hashVals)
	defer regCtx.releaseLatches(hashVals)

	status := store.rollbackKeyReadLock(lockBatch, key, startTS)
	if status != rollbackStatusDone {
		err := store.rollbackKeyReadDB(reqCtx, lockBatch, key, startTS, status == rollbackStatusNewLock)
		if err != nil {
			return err
		}
	}
	err := store.writeLocks(lockBatch)
	store.lockWaiterManager.WakeUp(startTS, 0, hashVals)
	return err
}

func (store *MVCCStore) ScanLock(reqCtx *requestCtx, maxSystemTS uint64) ([]*kvrpcpb.LockInfo, error) {
	var locks []*kvrpcpb.LockInfo
	if len(reqCtx.regCtx.endKey) == 0 {
		panic("invalid end key")
	}

	it := store.lockStore.NewIterator()
	for it.Seek(reqCtx.regCtx.startKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), reqCtx.regCtx.endKey) {
			return locks, nil
		}
		lock := mvcc.DecodeLock(it.Value())
		if lock.StartTS < maxSystemTS {
			locks = append(locks, &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.StartTS,
				Key:         codec.EncodeBytes(nil, it.Key()),
				LockTtl:     uint64(lock.TTL),
			})
		}
	}
	return locks, nil
}

func (store *MVCCStore) ResolveLock(reqCtx *requestCtx, startTS, commitTS uint64) error {
	regCtx := reqCtx.regCtx
	if len(regCtx.endKey) == 0 {
		panic("invalid end key")
	}
	var lockKeys [][]byte
	var lockVals [][]byte
	it := store.lockStore.NewIterator()
	var hasPessimisticLock bool
	for it.Seek(regCtx.startKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), regCtx.endKey) {
			break
		}
		lock := mvcc.DecodeLock(it.Value())
		if lock.StartTS != startTS {
			continue
		}
		if lock.Op == uint8(kvrpcpb.Op_PessimisticLock) {
			hasPessimisticLock = true
		}
		lockKeys = append(lockKeys, safeCopy(it.Key()))
		lockVals = append(lockVals, safeCopy(it.Value()))
	}
	if len(lockKeys) == 0 {
		return nil
	}
	if hasPessimisticLock {
		store.deadlockDetector.CleanUp(startTS)
	}

	hashVals := keysToHashVals(lockKeys...)
	lockBatch := newWriteLockBatch(reqCtx)
	var dbBatch *writeDBBatch
	var userMeta mvcc.DBUserMeta
	if commitTS > 0 {
		dbBatch = newWriteDBBatch(reqCtx)
		userMeta = mvcc.NewDBUserMeta(startTS, commitTS)
	}

	regCtx.acquireLatches(hashVals)
	defer regCtx.releaseLatches(hashVals)

	var buf []byte
	for i, lockKey := range lockKeys {
		buf = store.lockStore.Get(lockKey, buf)
		// We need to check again make sure the lock is not changed.
		if bytes.Equal(buf, lockVals[i]) {
			if commitTS > 0 {
				lock := mvcc.DecodeLock(lockVals[i])
				dbBatch.set(lockKey, lock.Value, userMeta)
			}
			lockBatch.delete(lockKey)
		}
	}
	if len(lockBatch.entries) == 0 {
		return nil
	}
	if dbBatch != nil {
		atomic.AddInt64(&regCtx.diff, dbBatch.size())
		err := store.writeDB(dbBatch)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err := store.writeLocks(lockBatch)
	store.lockWaiterManager.WakeUp(startTS, commitTS, hashVals)
	return errors.Trace(err)
}

const delRangeBatchSize = 4096

func (store *MVCCStore) DeleteRange(reqCtx *requestCtx, startKey, endKey []byte) error {
	keys := make([][]byte, 0, delRangeBatchSize)
	oldStartKey := mvcc.EncodeOldKey(startKey, maxSystemTS)
	oldEndKey := mvcc.EncodeOldKey(endKey, maxSystemTS)
	reader := reqCtx.getDBReader()
	keys = store.collectRangeKeys(reader.GetIter(), startKey, endKey, keys)
	keys = store.collectRangeKeys(reader.GetIter(), oldStartKey, oldEndKey, keys)
	err := store.deleteKeysInBatch(reqCtx, keys, delRangeBatchSize)
	if err != nil {
		log.Error(err)
	}
	return errors.Trace(err)
}

func (store *MVCCStore) collectRangeKeys(it *badger.Iterator, startKey, endKey []byte, keys [][]byte) [][]byte {
	if len(endKey) == 0 {
		panic("invalid end key")
	}
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
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

func (store *MVCCStore) deleteKeysInBatch(reqCtx *requestCtx, keys [][]byte, batchSize int) error {
	regCtx := reqCtx.regCtx
	for len(keys) > 0 {
		batchSize := mathutil.Min(len(keys), batchSize)
		batchKeys := keys[:batchSize]
		keys = keys[batchSize:]
		hashVals := keysToHashVals(batchKeys...)
		dbBatch := newWriteDBBatch(reqCtx)
		for _, key := range batchKeys {
			dbBatch.delete(key)
		}

		regCtx.acquireLatches(hashVals)
		err := store.writeDB(dbBatch)
		regCtx.releaseLatches(hashVals)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (store *MVCCStore) GC(reqCtx *requestCtx, safePoint uint64) error {
	// We use the gcLock to make sure safePoint can only increase.
	store.gcLock.Lock()
	defer store.gcLock.Unlock()
	oldSafePoint := atomic.LoadUint64(&store.safePoint.timestamp)
	if oldSafePoint < safePoint {
		err := store.db.Update(func(txn *badger.Txn) error {
			safePointValue := make([]byte, 8)
			binary.LittleEndian.PutUint64(safePointValue, safePoint)
			return txn.Set(InternalSafePointKey, safePointValue)
		})
		if err != nil {
			return err
		}
		atomic.StoreUint64(&store.safePoint.timestamp, safePoint)
	}
	return nil
}

type SafePoint struct {
	timestamp uint64
}

// CreateCompactionFilter implements badger.CompactionFilterFactory function.
func (sp *SafePoint) CreateCompactionFilter() badger.CompactionFilter {
	return &GCCompactionFilter{
		safePoint: atomic.LoadUint64(&sp.timestamp),
	}
}

// GCCompactionFilter implements the badger.CompactionFilter interface.
type GCCompactionFilter struct {
	safePoint uint64
}

// (old key first byte) = (latest key first byte) + 1
const (
	metaPrefix byte = 'm'
	// 'm' + 1 = 'n'
	metaOldPrefix byte = 'n'
	tablePrefix   byte = 't'
	// 't' + 1 = 'u
	tableOldPrefix byte = 'u'
)

// Filter implements the badger.CompactionFilter interface.
//
// The keys is divided into two sections, latest key and old key, the latest keys don't have commitTS appended,
// the old keys have commitTS appended and has a different key prefix.
//
// For old keys, if the nextCommitTS is before the safePoint, it means we already have one version before the safePoint,
// we can safely drop this entry.
//
// For latest keys, only Delete entry should be GCed, if the commitTS of the Delete entry is older than safePoint,
// we can remove the Delete entry. But we need to convert it to a tombstone instead of drop.
// Because there maybe multiple badger level old entries under the same key, dropping it results in old
// entries may appear again.
func (f *GCCompactionFilter) Filter(key, value, userMeta []byte) badger.Decision {
	switch key[0] {
	case metaPrefix, tablePrefix:
		// For latest version, we can only remove `delete` key, which has value len 0.
		if mvcc.DBUserMeta(userMeta).CommitTS() < f.safePoint && len(value) == 0 {
			return badger.DecisionMarkTombstone
		}
	case metaOldPrefix, tableOldPrefix:
		// The key is old version key.
		if mvcc.OldUserMeta(userMeta).NextCommitTS() < f.safePoint {
			return badger.DecisionDrop
		}
	}
	return badger.DecisionKeep
}
