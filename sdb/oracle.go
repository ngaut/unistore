package sdb

import (
	"math"
	"sync"
	"sync/atomic"
)

type oracle struct {
	// curRead must be at the Top for memory alignment. See issue #311.
	curRead  uint64 // Managed by the mutex.
	refCount int64

	sync.Mutex
	writeLock  sync.Mutex
	nextCommit uint64

	// commits stores a key fingerprint and latest commit counter for it.
	// refCount is used to clear out commits map to avoid a memory blowup.
	commits map[uint64]uint64
}

func (o *oracle) readTs() uint64 {
	return atomic.LoadUint64(&o.curRead)
}

func (o *oracle) commitTs() uint64 {
	o.Lock()
	defer o.Unlock()
	return o.nextCommit
}

func (o *oracle) allocTs() uint64 {
	o.Lock()
	ts := o.nextCommit
	o.nextCommit++
	o.Unlock()
	return ts
}

func (o *oracle) doneCommit(cts uint64) {
	for {
		curRead := atomic.LoadUint64(&o.curRead)
		if cts <= curRead {
			return
		}
		atomic.CompareAndSwapUint64(&o.curRead, curRead, cts)
	}
}

type safeTsTracker struct {
	safeTs uint64

	maxInactive uint64
	minActive   uint64
}

func (t *safeTsTracker) Begin() {
	// t.maxInactive = 0
	t.minActive = math.MaxUint64
}

func (t *safeTsTracker) Inspect(payload interface{}, isActive bool) {
	ts, ok := payload.(uint64)
	if !ok {
		return
	}

	if isActive {
		if ts < t.minActive {
			t.minActive = ts
		}
	} else {
		if ts > t.maxInactive {
			t.maxInactive = ts
		}
	}
}

func (t *safeTsTracker) End() {
	var safe uint64
	if t.minActive == math.MaxUint64 {
		safe = t.maxInactive
	} else {
		safe = t.minActive - 1
	}

	if safe > atomic.LoadUint64(&t.safeTs) {
		atomic.StoreUint64(&t.safeTs, safe)
	}
}
