package lockwaiter

import (
	"sync"
	"time"
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
	waiters []chan uint64
}

type Waiter struct {
	timeout time.Duration
	ch      chan uint64
}

func (w *Waiter) Wait() (unlocked bool, commitTS uint64) {
	select {
	case <-time.After(w.timeout):
		return false, 0
	case commitTS = <-w.ch:
		return true, commitTS
	}
}

// Wait waits on a lock until waked by others or timeout.
func (lw *Manager) NewWaiter(lockTS uint64, timeout time.Duration) *Waiter {
	// allocate memory before hold the lock.
	q := new(queue)
	ch := make(chan uint64, 1)
	q.waiters = make([]chan uint64, 0, 8)
	q.waiters = append(q.waiters, ch)
	lw.mu.Lock()
	if old, ok := lw.waitingQueues[lockTS]; ok {
		old.waiters = append(old.waiters, ch)
	} else {
		lw.waitingQueues[lockTS] = q
	}
	lw.mu.Unlock()
	return &Waiter{
		timeout: timeout,
		ch:      ch,
	}
}

// WakeUp wakes up waiters that waiting on the transaction.
func (lw *Manager) WakeUp(txn uint64, commitTS uint64) {
	lw.mu.Lock()
	q := lw.waitingQueues[txn]
	if q != nil {
		delete(lw.waitingQueues, txn)
	}
	lw.mu.Unlock()
	if q != nil {
		for _, ch := range q.waiters {
			ch <- commitTS
		}
	}
}
