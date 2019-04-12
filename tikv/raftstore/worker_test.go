package raftstore

import (
	"github.com/ngaut/unistore/lockstore"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestStalePeerInfo(t *testing.T) {
	time := time.Now()
	peerInfo := &stalePeerInfo{
		regionId: 1,
		endKey: []byte{1, 2, 10},
		timeout: time,
	}
	data := peerInfo.serializeStalePeerInfo()
	another := deserializeStalePeerInfo(data)
	assert.Equal(t, another.regionId, peerInfo.regionId)
	assert.Equal(t, another.endKey, peerInfo.endKey)
	assert.Equal(t, another.regionId, peerInfo.regionId)
	assert.True(t, peerInfo.timeout.Equal(another.timeout))
}

func insertRange(delRanges *pendingDeleteRanges, id uint64, s, e string, timeout time.Time) {
	delRanges.insert(id, []byte(s), []byte(e), timeout)
}

func TestPendingDeleteRanges(t *testing.T) {
	delRange := &pendingDeleteRanges{
		ranges: lockstore.NewMemStore(1024),
	}
	delay := time.Millisecond * 100
	id := uint64(0)
	timeout := time.Now().Add(delay)

	insertRange(delRange, id, "a", "c", timeout)
	insertRange(delRange, id, "m", "n", timeout)
	insertRange(delRange, id, "x", "z", timeout)
	insertRange(delRange, id + 1, "f", "i", timeout)
	insertRange(delRange, id + 1, "p", "t", timeout)
	assert.Equal(t, delRange.ranges.Len(), 5)

	time.Sleep(delay / 2)

	//  a____c    f____i    m____n    p____t    x____z
	//              g___________________q
	// when we want to insert [g, q), we first extract overlap ranges,
	// which are [f, i), [m, n), [p, t)
	timeout = time.Now().Add(delay)
	overlapRanges := delRange.drainOverlapRanges([]byte{'g'}, []byte{'q'})
	assert.Equal(t, overlapRanges, []delRangeHolder{
		{regionId: id + 1, startKey: []byte{'f'}, endKey: []byte{'i'}},
		{regionId: id, startKey: []byte{'m'}, endKey: []byte{'n'}},
		{regionId: id + 1, startKey: []byte{'p'}, endKey: []byte{'t'}},
	})

	assert.Equal(t, delRange.ranges.Len(), 2)
	insertRange(delRange, id + 2, "g", "q", timeout)
	assert.Equal(t, delRange.ranges.Len(), 3)
	time.Sleep(delay / 2)

	// at t1, [a, c) and [x, z) will timeout
	now := time.Now()
	ranges := delRange.timeoutRanges(now)
	assert.Equal(t, ranges, []delRangeHolder{
		{regionId: id, startKey: []byte{'a'}, endKey: []byte{'c'}},
		{regionId: id, startKey: []byte{'x'}, endKey: []byte{'z'}},
	})

	for _, r := range ranges {
		delRange.remove(r.startKey)
	}
	assert.Equal(t, delRange.ranges.Len(), 1)

	time.Sleep(delay / 2)

	// at t2, [g, q) will timeout
	now = time.Now()
	ranges = delRange.timeoutRanges(now)
	assert.Equal(t, ranges, []delRangeHolder{
		{regionId: id + 2, startKey: []byte{'g'}, endKey: []byte{'q'}},
	})
	for _, r := range ranges {
		delRange.remove(r.startKey)
	}
	assert.Equal(t, delRange.ranges.Len(), 0)
}
