package lockwaiter

import (
	"sort"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/ngaut/unistore/config"
	"github.com/pingcap/kvproto/pkg/deadlock"
)

type Manager struct {
	mu            sync.Mutex
	waitingQueues map[uint64]*queue
}

func NewManager() *Manager {
	return &Manager{
		waitingQueues: map[uint64]*queue{},
	}
}

type queue struct {
	waiters []*Waiter
}

// getReadyWaiters returns the ready waiters array, and left waiter size in this queue,
// it should be used under map lock protection
func (q *queue) getReadyWaiters(keyHashes []uint64) (readyWaiters []*Waiter, remainSize int) {
	readyWaiters = make([]*Waiter, 0, 8)
	remainedWaiters := q.waiters[:0]
	for _, w := range q.waiters {
		if w.inKeys(keyHashes) {
			readyWaiters = append(readyWaiters, w)
		} else {
			remainedWaiters = append(remainedWaiters, w)
		}
	}
	remainSize = len(remainedWaiters)
	q.waiters = remainedWaiters
	return
}

// removeWaiter removes the correspond waiter from pending array
// it should be used under map lock protection
func (q *queue) removeWaiter(w *Waiter) {
	for i, waiter := range q.waiters {
		if waiter == w {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			break
		}
	}
}

type Waiter struct {
	timeout time.Duration
	ch      chan WaitResult
	startTS uint64
	LockTS  uint64
	KeyHash uint64
}

// WakeupWaitTime is the implementation of variable "wake-up-delay-duration"
type WakeupWaitTime int

type WaitResult struct {
	// WakeupSleepTime, -1 means the wait is already timeout, 0 means the lock will be granted to this waiter
	// others are the wake-up-delay-duration sleep time, in milliseconds
	WakeupSleepTime WakeupWaitTime
	CommitTS        uint64
	DeadlockResp    *deadlock.DeadlockResponse
}

const WaitTimeout WakeupWaitTime = -1
const WakeUpThisWaiter WakeupWaitTime = 0

func (w *Waiter) Wait() WaitResult {
	select {
	case <-time.After(w.timeout):
		return WaitResult{WakeupSleepTime: WaitTimeout}
	case result := <-w.ch:
		return result
	}
}

func (w *Waiter) inKeys(keyHashes []uint64) bool {
	idx := sort.Search(len(keyHashes), func(i int) bool {
		return keyHashes[i] >= w.KeyHash
	})
	if idx == len(keyHashes) {
		return false
	}
	return keyHashes[idx] == w.KeyHash
}

// Wait waits on a lock until waked by others or timeout.
func (lw *Manager) NewWaiter(startTS, lockTS, keyHash uint64, timeout time.Duration) *Waiter {
	// allocate memory before hold the lock.
	q := new(queue)
	q.waiters = make([]*Waiter, 0, 8)
	waiter := &Waiter{
		timeout: timeout,
		ch:      make(chan WaitResult, 1),
		startTS: startTS,
		LockTS:  lockTS,
		KeyHash: keyHash,
	}
	q.waiters = append(q.waiters, waiter)
	lw.mu.Lock()
	if old, ok := lw.waitingQueues[lockTS]; ok {
		old.waiters = append(old.waiters, waiter)
	} else {
		lw.waitingQueues[lockTS] = q
	}
	lw.mu.Unlock()
	return waiter
}

// WakeUp wakes up waiters that waiting on the transaction.
func (lw *Manager) WakeUp(txn, commitTS uint64, keyHashes []uint64) {
	var (
		waiters    []*Waiter
		remainSize int
	)
	lw.mu.Lock()
	q := lw.waitingQueues[txn]
	if q != nil {
		sort.Slice(keyHashes, func(i, j int) bool {
			return keyHashes[i] < keyHashes[j]
		})
		waiters, remainSize = q.getReadyWaiters(keyHashes)
		if remainSize == 0 {
			delete(lw.waitingQueues, txn)
		}
	}
	lw.mu.Unlock()

	// wake up waiters
	if len(waiters) > 0 {
		wokenUpMap := make(map[uint64]struct{})
		// make the waiters in star
		sort.Slice(waiters, func(i, j int) bool {
			return waiters[i].startTS < waiters[j].startTS
		})
		for _, w := range waiters {
			wakeUpDelay := WakeupWaitTime(config.DefaultConf.PessimisticTxn.WakeUpDelayDuration)
			if _, ok := wokenUpMap[w.KeyHash]; !ok {
				wakeUpDelay = WakeUpThisWaiter
				wokenUpMap[w.KeyHash] = struct{}{}
			}
			w.ch <- WaitResult{WakeupSleepTime: wakeUpDelay, CommitTS: commitTS}
		}
		log.Info("wakeup", len(waiters), "txns blocked by txn", txn, " keyHashes=", keyHashes)
	}
}

// CleanUp removes a waiter from waitingQueues when wait timeout.
func (lw *Manager) CleanUp(w *Waiter) {
	lw.mu.Lock()
	q := lw.waitingQueues[w.LockTS]
	if q != nil {
		q.removeWaiter(w)
		if len(q.waiters) == 0 {
			delete(lw.waitingQueues, w.LockTS)
		}
	}
	lw.mu.Unlock()
}

// WakeUpDetection wakes up waiters waiting for deadlock detection results
func (lw *Manager) WakeUpForDeadlock(resp *deadlock.DeadlockResponse) {
	var (
		waiter     *Waiter
		waitForTxn uint64
	)
	waitForTxn = resp.Entry.WaitForTxn
	lw.mu.Lock()
	q := lw.waitingQueues[waitForTxn]
	if q != nil {
		for i, curWaiter := range q.waiters {
			// there should be no duplicated waiters
			if curWaiter.startTS == resp.Entry.Txn && curWaiter.KeyHash == resp.Entry.KeyHash {
				log.Infof("deadlock detection response got for entry=%v", resp.Entry)
				waiter = curWaiter
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				break
			}
		}
		if len(q.waiters) == 0 {
			delete(lw.waitingQueues, waitForTxn)
		}
	}
	lw.mu.Unlock()
	if waiter != nil {
		waiter.ch <- WaitResult{DeadlockResp: resp}
		log.Infof("wakeup txn=%v blocked by txn=%v because of deadlock, keyHash=%v, deadlockKeyHash=%v",
			resp.Entry.Txn, waitForTxn, resp.Entry.KeyHash, resp.DeadlockKeyHash)
	}
}
