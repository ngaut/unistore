package tikv

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coocood/badger"
	"github.com/dgryski/go-farm"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/rowcodec"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/ngaut/unistore/util/lockwaiter"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/deadlock"
)

// MVCCStore is a wrapper of badger.DB to provide MVCC functions.
type MVCCStore struct {
	dir           string
	db            *badger.DB
	lockStore     *lockstore.MemStore
	rollbackStore *lockstore.MemStore
	dbWriter      mvcc.DBWriter

	safePoint *SafePoint
	gcLock    sync.Mutex

	latestTS          uint64
	deadlockDetector  *deadlock.Detector
	lockWaiterManager *lockwaiter.Manager
}

// NewMVCCStore creates a new MVCCStore
func NewMVCCStore(bundle *mvcc.DBBundle, dataDir string, safePoint *SafePoint, writer mvcc.DBWriter) *MVCCStore {
	store := &MVCCStore{
		db:                bundle.DB,
		dir:               dataDir,
		lockStore:         bundle.LockStore,
		rollbackStore:     bundle.RollbackStore,
		safePoint:         safePoint,
		dbWriter:          writer,
		deadlockDetector:  deadlock.NewDetector(),
		lockWaiterManager: lockwaiter.NewManager(),
	}
	store.loadSafePoint()
	writer.Open()
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

func (store *MVCCStore) updateLatestTS(ts uint64) {
	for {
		old := atomic.LoadUint64(&store.latestTS)
		if old < ts {
			if !atomic.CompareAndSwapUint64(&store.latestTS, old, ts) {
				continue
			}
		}
		return
	}
}

func (store *MVCCStore) getLatestTS() uint64 {
	return atomic.LoadUint64(&store.latestTS)
}

func (store *MVCCStore) Close() error {
	store.dbWriter.Close()

	err := store.dumpMemLocks()
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

type lockEntryHdr struct {
	keyLen uint32
	valLen uint32
}

func (store *MVCCStore) dumpMemLocks() error {
	tmpFileName := store.dir + "/lock_store.tmp"
	f, err := os.OpenFile(tmpFileName, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		return errors.Trace(err)
	}
	writer := bufio.NewWriter(f)
	cnt := 0
	it := store.lockStore.NewIterator()
	hdrBuf := make([]byte, 8)
	hdr := (*lockEntryHdr)(unsafe.Pointer(&hdrBuf[0]))
	for it.SeekToFirst(); it.Valid(); it.Next() {
		hdr.keyLen = uint32(len(it.Key()))
		hdr.valLen = uint32(len(it.Value()))
		writer.Write(hdrBuf)
		writer.Write(it.Key())
		writer.Write(it.Value())
		cnt++
	}
	err = writer.Flush()
	if err != nil {
		return errors.Trace(err)
	}
	err = f.Sync()
	if err != nil {
		return errors.Trace(err)
	}
	f.Close()
	return os.Rename(tmpFileName, store.dir+"/lock_store")
}

func (store *MVCCStore) getDBItems(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation) (items []*badger.Item, err error) {
	txn := reqCtx.getDBReader().GetTxn()
	keys := make([][]byte, len(mutations))
	for i, m := range mutations {
		keys[i] = m.Key
	}
	return txn.MultiGet(keys)
}

func (store *MVCCStore) PessimisticLock(reqCtx *requestCtx, req *kvrpcpb.PessimisticLockRequest) (*lockwaiter.Waiter, error) {
	mutations := req.Mutations
	startTS := req.StartVersion
	regCtx := reqCtx.regCtx
	hashVals := mutationsToHashVals(mutations)
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)
	for _, m := range mutations {
		lock, err := store.checkConflictInLockStore(reqCtx, m, startTS)
		if err != nil {
			return store.handleCheckPessimisticErr(startTS, err, req.IsFirstLock)
		}
		if lock != nil {
			if lock.Op != uint8(kvrpcpb.Op_PessimisticLock) {
				return nil, errors.New("lock type not match")
			}
			if lock.ForUpdateTS >= req.ForUpdateTs {
				// It's a duplicate command, we can return directly.
				return nil, nil
			}
			// Single statement rollback key, we can overwrite it.
			batch.PessimisticRollback(m.Key)
		}
	}
	items, err := store.getDBItems(reqCtx, mutations)
	if err != nil {
		return nil, err
	}
	for i, m := range mutations {
		lock, err1 := store.buildPessimisticLock(m, items[i], req)
		if err1 != nil {
			return nil, err1
		}
		batch.PessimisticLock(m.Key, lock)
	}
	err = store.dbWriter.Write(batch)
	return nil, err
}

func (store *MVCCStore) PessimisticRollback(reqCtx *requestCtx, req *kvrpcpb.PessimisticRollbackRequest) error {
	keys := req.Keys
	hashVals := keysToHashVals(keys...)
	regCtx := reqCtx.regCtx
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)
	startTS := req.StartVersion
	var batch mvcc.WriteBatch
	for _, k := range keys {
		lock := store.getLock(reqCtx, k)
		if lock != nil &&
			lock.Op == uint8(kvrpcpb.Op_PessimisticLock) &&
			lock.StartTS == startTS &&
			lock.ForUpdateTS <= req.ForUpdateTs {
			if batch == nil {
				batch = store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)
			}
			batch.PessimisticRollback(k)
		}
	}
	var err error
	if batch != nil {
		err = store.dbWriter.Write(batch)
	}
	store.lockWaiterManager.WakeUp(startTS, 0, hashVals)
	store.deadlockDetector.CleanUp(startTS)
	return err
}

func (store *MVCCStore) handleCheckPessimisticErr(startTS uint64, err error, isFirstLock bool) (*lockwaiter.Waiter, error) {
	if lock, ok := err.(*ErrLocked); ok {
		keyHash := farm.Fingerprint64(lock.Key)
		if !isFirstLock { // No need to detect deadlock if it's first lock.
			if err1 := store.deadlockDetector.Detect(startTS, lock.StartTS, keyHash); err1 != nil {
				return nil, &ErrDeadlock{
					LockKey:         lock.Key,
					LockTS:          lock.StartTS,
					DeadlockKeyHash: *(*uint64)(unsafe.Pointer(err1)), // TODO: update here when keyHash is exported.
				}
			}
		}
		log.Infof("%d blocked by %d on key %d", startTS, lock.StartTS, keyHash)
		waiter := store.lockWaiterManager.NewWaiter(startTS, lock.StartTS, farm.Fingerprint64(lock.Key), time.Second)
		return waiter, err
	}
	return nil, err
}

func (store *MVCCStore) buildPessimisticLock(m *kvrpcpb.Mutation, item *badger.Item,
	req *kvrpcpb.PessimisticLockRequest) (*mvcc.MvccLock, error) {
	if item != nil {
		userMeta := mvcc.DBUserMeta(item.UserMeta())
		if userMeta.CommitTS() > req.ForUpdateTs {
			return nil, &ErrConflict{
				StartTS:          req.StartVersion,
				ConflictTS:       userMeta.StartTS(),
				ConflictCommitTS: userMeta.CommitTS(),
				Key:              item.KeyCopy(nil),
			}
		}
		if m.Assertion == kvrpcpb.Assertion_NotExist && !item.IsEmpty() {
			return nil, &ErrKeyAlreadyExists{Key: m.Key}
		}
	}
	lock := &mvcc.MvccLock{
		MvccLockHdr: mvcc.MvccLockHdr{
			StartTS:     req.StartVersion,
			ForUpdateTS: req.ForUpdateTs,
			Op:          uint8(kvrpcpb.Op_PessimisticLock),
			TTL:         uint32(req.LockTtl),
			PrimaryLen:  uint16(len(req.PrimaryLock)),
		},
		Primary: req.PrimaryLock,
	}
	return lock, nil
}

func (store *MVCCStore) Prewrite(reqCtx *requestCtx, req *kvrpcpb.PrewriteRequest) error {
	mutations := req.Mutations
	regCtx := reqCtx.regCtx
	hashVals := mutationsToHashVals(mutations)

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	isPessimistic := req.ForUpdateTs > 0
	if isPessimistic {
		return store.prewritePessimistic(reqCtx, mutations, req)
	}
	return store.prewriteOptimistic(reqCtx, mutations, req)
}

func (store *MVCCStore) prewriteOptimistic(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation, req *kvrpcpb.PrewriteRequest) error {
	startTS := req.StartVersion
	// Must check the LockStore first.
	for _, m := range mutations {
		lock, err := store.checkConflictInLockStore(reqCtx, m, startTS)
		if err != nil {
			return err
		}
		if lock != nil {
			// duplicated command
			return nil
		}
	}
	items, err := store.getDBItems(reqCtx, mutations)
	if err != nil {
		return err
	}
	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)
	for i, m := range mutations {
		item := items[i]
		if item != nil {
			userMeta := mvcc.DBUserMeta(item.UserMeta())
			if userMeta.CommitTS() > startTS {
				return &ErrConflict{
					StartTS:          startTS,
					ConflictTS:       userMeta.StartTS(),
					ConflictCommitTS: userMeta.CommitTS(),
					Key:              item.KeyCopy(nil),
				}
			}
		}
		lock, err1 := store.buildPrewriteLock(reqCtx, m, items[i], req)
		if err1 != nil {
			return err1
		}
		batch.Prewrite(m.Key, lock, false)
	}
	return store.dbWriter.Write(batch)
}

func (store *MVCCStore) prewritePessimistic(reqCtx *requestCtx, mutations []*kvrpcpb.Mutation, req *kvrpcpb.PrewriteRequest) error {
	startTS := req.StartVersion
	for i, m := range mutations {
		lock := store.getLock(reqCtx, m.Key)
		isPessimisticLock := len(req.IsPessimisticLock) > 0 && req.IsPessimisticLock[i]
		lockExists := lock != nil
		lockMatch := lockExists && lock.StartTS == startTS
		if isPessimisticLock {
			valid := lockExists && lockMatch
			if !valid {
				return errors.New("pessimistic lock not found")
			}
			if lock.Op != uint8(kvrpcpb.Op_PessimisticLock) {
				// Duplicated command.
				return nil
			}
		} else {
			// non pessimistic lock in pessimistic transaction, e.g. non-unique index.
			valid := !lockExists || lockMatch
			if !valid {
				// Safe to set TTL to zero because the transaction of the lock is committed
				// or rollbacked or must be rollbacked.
				return &ErrLocked{
					Key:     m.Key,
					Primary: lock.Primary,
					StartTS: lock.StartTS,
					TTL:     0,
				}
			}
			if lockMatch {
				// Duplicate command.
				return nil
			}
		}
	}
	items, err := store.getDBItems(reqCtx, mutations)
	if err != nil {
		return err
	}
	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)
	for i, m := range mutations {
		lock, err1 := store.buildPrewriteLock(reqCtx, m, items[i], req)
		if err1 != nil {
			return err1
		}
		batch.Prewrite(m.Key, lock, len(req.IsPessimisticLock) > 0 && req.IsPessimisticLock[i])
	}
	return store.dbWriter.Write(batch)
}

func (store *MVCCStore) buildPrewriteLock(reqCtx *requestCtx, m *kvrpcpb.Mutation, item *badger.Item,
	req *kvrpcpb.PrewriteRequest) (*mvcc.MvccLock, error) {
	lock := &mvcc.MvccLock{
		MvccLockHdr: mvcc.MvccLockHdr{
			StartTS:    req.StartVersion,
			TTL:        uint32(req.LockTtl),
			PrimaryLen: uint16(len(req.PrimaryLock)),
		},
		Primary: req.PrimaryLock,
		Value:   m.Value,
	}
	var err error
	if item != nil {
		lock.HasOldVer = true
		lock.OldMeta = mvcc.DBUserMeta(item.UserMeta())
		lock.OldVal, err = item.Value()
		if err != nil {
			return nil, err
		}
	}
	lock.Op = uint8(m.Op)
	if lock.Op == uint8(kvrpcpb.Op_Insert) {
		if lock.HasOldVer && len(lock.OldVal) > 0 {
			return nil, &ErrKeyAlreadyExists{Key: m.Key}
		}
		lock.Op = uint8(kvrpcpb.Op_Put)
	}
	if rowcodec.IsRowKey(m.Key) && lock.Op == uint8(kvrpcpb.Op_Put) {
		var enc rowcodec.Encoder
		reqCtx.buf, err = enc.EncodeFromOldRow(m.Value, reqCtx.buf)
		if err != nil {
			log.Errorf("err:%v m.Value:%v m.Key:%q m.Op:%d", err, m.Value, m.Key, m.Op)
			return nil, err
		}
		lock.Value = reqCtx.buf
	}

	lock.ForUpdateTS = req.ForUpdateTs
	return lock, nil
}

func (store *MVCCStore) getLock(req *requestCtx, key []byte) *mvcc.MvccLock {
	req.buf = store.lockStore.Get(key, req.buf)
	if len(req.buf) == 0 {
		return nil
	}
	lock := mvcc.DecodeLock(req.buf)
	return &lock
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

const maxSystemTS uint64 = math.MaxUint64

// Commit implements the MVCCStore interface.
func (store *MVCCStore) Commit(req *requestCtx, keys [][]byte, startTS, commitTS uint64) error {
	store.updateLatestTS(commitTS)
	regCtx := req.regCtx
	hashVals := keysToHashVals(keys...)
	batch := store.dbWriter.NewWriteBatch(startTS, commitTS, req.rpcCtx)
	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	var buf []byte
	var tmpDiff int
	var isPessimisticTxn bool
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
		isPessimisticTxn = lock.ForUpdateTS > 0
		tmpDiff += len(key) + len(lock.Value)
		batch.Commit(key, &lock)
	}
	atomic.AddInt64(&regCtx.diff, int64(tmpDiff))
	err := store.dbWriter.Write(batch)
	store.lockWaiterManager.WakeUp(startTS, commitTS, hashVals)
	if isPessimisticTxn {
		store.deadlockDetector.CleanUp(startTS)
	}
	return err
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
	hashVals := keysToHashVals(keys...)
	log.Warnf("%d rollback %v", startTS, hashVals)
	regCtx := reqCtx.regCtx
	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	statuses := make([]int, len(keys))
	for i, key := range keys {
		statuses[i] = store.rollbackKeyReadLock(reqCtx, batch, key, startTS)
	}
	for i, key := range keys {
		status := statuses[i]
		if status == rollbackStatusDone || status == rollbackPessimistic {
			// rollback pessimistic lock doesn't need to read db.
			continue
		}
		err := store.rollbackKeyReadDB(reqCtx, batch, key, startTS, statuses[i] == rollbackStatusNewLock)
		if err != nil {
			return err
		}
	}
	store.deadlockDetector.CleanUp(startTS)
	err := store.dbWriter.Write(batch)
	return errors.Trace(err)
}

func (store *MVCCStore) rollbackKeyReadLock(reqCtx *requestCtx, batch mvcc.WriteBatch, key []byte, startTS uint64) (status int) {
	reqCtx.buf = mvcc.EncodeRollbackKey(reqCtx.buf, key, startTS)
	rollbackKey := safeCopy(reqCtx.buf)
	reqCtx.buf = store.rollbackStore.Get(rollbackKey, reqCtx.buf)
	if len(reqCtx.buf) != 0 {
		// Already rollback.
		return rollbackStatusDone
	}
	reqCtx.buf = store.lockStore.Get(key, reqCtx.buf)
	hasLock := len(reqCtx.buf) > 0
	if hasLock {
		lock := mvcc.DecodeLock(reqCtx.buf)
		if lock.StartTS < startTS {
			// The lock is old, means this is written by an old transaction, and the current transaction may not arrive.
			// We should write a rollback lock.
			batch.Rollback(key, false)
			return rollbackStatusDone
		}
		if lock.StartTS == startTS {
			if lock.Op == uint8(kvrpcpb.Op_PessimisticLock) {
				batch.PessimisticRollback(key)
				return rollbackPessimistic
			}
			// We can not simply delete the lock because the prewrite may be sent multiple times.
			// To prevent that we update it a rollback lock.
			batch.Rollback(key, true)
			return rollbackStatusDone
		}
		// lock.startTS > startTS, go to DB to check if the key is committed.
		return rollbackStatusNewLock
	}
	return rollbackStatusNoLock
}

func (store *MVCCStore) rollbackKeyReadDB(req *requestCtx, batch mvcc.WriteBatch, key []byte, startTS uint64, hasLock bool) error {
	reader := req.getDBReader()
	item, err := reader.GetTxn().Get(key)
	if err != nil && err != badger.ErrKeyNotFound {
		return errors.Trace(err)
	}
	hasVal := item != nil
	if !hasVal && !hasLock {
		// The prewrite request is not arrived, we write a rollback lock to prevent the future prewrite.
		batch.Rollback(key, false)
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
	// look for the key in the old version to check if the key is committed.
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
	// commit not found, rollback this key
	batch.Rollback(key, false)
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
	hashVals := keysToHashVals(key)
	regCtx := reqCtx.regCtx
	batch := store.dbWriter.NewWriteBatch(startTS, 0, reqCtx.rpcCtx)

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	status := store.rollbackKeyReadLock(reqCtx, batch, key, startTS)
	if status != rollbackStatusDone {
		err := store.rollbackKeyReadDB(reqCtx, batch, key, startTS, status == rollbackStatusNewLock)
		if err != nil {
			return err
		}
	}
	err := store.dbWriter.Write(batch)
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
	for it.Seek(regCtx.startKey); it.Valid(); it.Next() {
		if exceedEndKey(it.Key(), regCtx.endKey) {
			break
		}
		lock := mvcc.DecodeLock(it.Value())
		if lock.StartTS != startTS {
			continue
		}
		lockKeys = append(lockKeys, safeCopy(it.Key()))
		lockVals = append(lockVals, safeCopy(it.Value()))
	}
	if len(lockKeys) == 0 {
		return nil
	}
	hashVals := keysToHashVals(lockKeys...)
	batch := store.dbWriter.NewWriteBatch(startTS, commitTS, reqCtx.rpcCtx)

	regCtx.AcquireLatches(hashVals)
	defer regCtx.ReleaseLatches(hashVals)

	var buf []byte
	var tmpDiff int
	for i, lockKey := range lockKeys {
		buf = store.lockStore.Get(lockKey, buf)
		// We need to check again make sure the lock is not changed.
		if bytes.Equal(buf, lockVals[i]) {
			if commitTS > 0 {
				lock := mvcc.DecodeLock(lockVals[i])
				tmpDiff += len(lockKey) + len(lock.Value)
				batch.Commit(lockKey, &lock)
			} else {
				batch.Rollback(lockKey, true)
			}
		}
	}
	atomic.AddInt64(&regCtx.diff, int64(tmpDiff))
	err := store.dbWriter.Write(batch)
	return err
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
func (sp *SafePoint) CreateCompactionFilter(targetLevel int, startKey, endKey []byte) badger.CompactionFilter {
	return &GCCompactionFilter{
		targetLevel: targetLevel,
		safePoint:   atomic.LoadUint64(&sp.timestamp),
	}
}

// GCCompactionFilter implements the badger.CompactionFilter interface.
type GCCompactionFilter struct {
	targetLevel int
	safePoint   uint64
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

var (
	baseGuard          = badger.Guard{MatchLen: 64, MinSize: 64 * 1024}
	raftGuard          = badger.Guard{Prefix: []byte{0}, MatchLen: 1, MinSize: 64 * 1024}
	metaGuard          = badger.Guard{Prefix: []byte{'m'}, MatchLen: 1, MinSize: 64 * 1024}
	metaOldGuard       = badger.Guard{Prefix: []byte{'n'}, MatchLen: 1, MinSize: 64 * 1024}
	tableGuard         = badger.Guard{Prefix: []byte{'t'}, MatchLen: 9, MinSize: 1 * 1024 * 1024}
	tableOldGuard      = badger.Guard{Prefix: []byte{'u'}, MatchLen: 9, MinSize: 1 * 1024 * 1024}
	tableIndexGuard    = badger.Guard{Prefix: []byte{'t'}, MatchLen: 11, MinSize: 1 * 1024 * 1024}
	tableIndexOldGuard = badger.Guard{Prefix: []byte{'u'}, MatchLen: 11, MinSize: 1 * 1024 * 1024}
)

func (f *GCCompactionFilter) Guards() []badger.Guard {
	if f.targetLevel < 4 {
		// do not split index and row for top levels.
		return []badger.Guard{
			baseGuard, raftGuard, metaGuard, metaOldGuard, tableGuard, tableOldGuard,
		}
	}
	// split index and row for bottom levels.
	return []badger.Guard{
		baseGuard, raftGuard, metaGuard, metaOldGuard, tableIndexGuard, tableIndexOldGuard,
	}
}
