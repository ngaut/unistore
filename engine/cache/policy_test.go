package cache

import (
	"testing"
	"time"
)

func TestPolicy(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("newPolicy failed")
		}
	}()
	newPolicy(100, 10)
}

func TestPolicyMetrics(t *testing.T) {
	p := newPolicy(100, 10)
	p.CollectMetrics(newMetrics())
	if p.metrics == nil || p.evict.metrics == nil {
		t.Fatal("policy metrics initialization error")
	}
}

func TestPolicyProcessItems(t *testing.T) {
	p := newPolicy(100, 10)
	p.itemsCh <- makeKeys(1, 2, 2)
	time.Sleep(wait)
	p.Lock()
	if p.admit.Estimate(iKey(2)) != 2 || p.admit.Estimate(iKey(1)) != 1 {
		p.Unlock()
		t.Fatal("policy processItems not pushing to tinylfu counters")
	}
	p.Unlock()
	p.stop <- struct{}{}
	p.itemsCh <- makeKeys(3, 3, 3)
	time.Sleep(wait)
	p.Lock()
	if p.admit.Estimate(iKey(3)) != 0 {
		p.Unlock()
		t.Fatal("policy processItems not stopping")
	}
	p.Unlock()
}

func TestPolicyPush(t *testing.T) {
	p := newPolicy(100, 10)
	if !p.Push([]Key{}) {
		t.Fatal("push empty slice should be good")
	}
	keepCount := 0
	for i := 0; i < 10; i++ {
		if p.Push(makeKeys(1, 2, 3, 4, 5)) {
			keepCount++
		}
	}
	if keepCount == 0 {
		t.Fatal("push dropped everything")
	}
}

func TestPolicyAdd(t *testing.T) {
	p := newPolicy(1000, 100)
	if victims, added := p.Add(iKey(1), 101); victims != nil || added {
		t.Fatal("can't add an item bigger than entire cache")
	}
	p.Lock()
	p.evict.add(iKey(1), 1)
	p.admit.Increment(iKey(1))
	p.admit.Increment(iKey(2))
	p.admit.Increment(iKey(3))
	p.Unlock()
	if victims, added := p.Add(iKey(1), 1); victims != nil || !added {
		t.Fatal("item should already exist")
	}
	if victims, added := p.Add(iKey(2), 20); victims != nil || !added {
		t.Fatal("item should be added with no eviction")
	}
	if victims, added := p.Add(iKey(3), 90); victims == nil || !added {
		t.Fatal("item should be added with eviction")
	}
	if victims, added := p.Add(iKey(4), 20); victims == nil || added {
		t.Fatal("item should not be added")
	}
}

func TestPolicyHas(t *testing.T) {
	p := newPolicy(100, 10)
	p.Add(iKey(1), 1)
	if !p.Has(iKey(1)) {
		t.Fatal("policy should have key")
	}
	if p.Has(iKey(2)) {
		t.Fatal("policy shouldn't have key")
	}
}

func TestPolicyDel(t *testing.T) {
	p := newPolicy(100, 10)
	p.Add(iKey(1), 1)
	p.Del(iKey(1))
	p.Del(iKey(2))
	if p.Has(iKey(1)) {
		t.Fatal("del didn't delete")
	}
	if p.Has(iKey(2)) {
		t.Fatal("policy shouldn't have key")
	}
}

func TestPolicyCap(t *testing.T) {
	p := newPolicy(100, 10)
	p.Add(iKey(1), 1)
	if p.Cap() != 9 {
		t.Fatal("cap returned wrong value")
	}
}

func TestPolicyUpdate(t *testing.T) {
	p := newPolicy(100, 10)
	p.Add(iKey(1), 1)
	p.Update(iKey(1), 2)
	p.Lock()
	if p.evict.keyCosts[iKey(1)] != 2 {
		p.Unlock()
		t.Fatal("update failed")
	}
	p.Unlock()
}

func TestPolicyCost(t *testing.T) {
	p := newPolicy(100, 10)
	p.Add(iKey(1), 2)
	if p.Cost(iKey(1)) != 2 {
		t.Fatal("cost for existing key returned wrong value")
	}
	if p.Cost(iKey(2)) != -1 {
		t.Fatal("cost for missing key returned wrong value")
	}
}

func TestPolicyClear(t *testing.T) {
	p := newPolicy(100, 10)
	p.Add(iKey(1), 1)
	p.Add(iKey(2), 2)
	p.Add(iKey(3), 3)
	p.Clear()
	if p.Cap() != 10 || p.Has(iKey(1)) || p.Has(iKey(2)) || p.Has(iKey(3)) {
		t.Fatal("clear didn't clear properly")
	}
}

func TestPolicyClose(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("close didn't close channels")
		}
	}()
	p := newPolicy(100, 10)
	p.Add(iKey(1), 1)
	p.Close()
	p.itemsCh <- makeKeys(1)
}

func TestSampledLFUAdd(t *testing.T) {
	e := newSampledLFU(4)
	e.add(iKey(1), 1)
	e.add(iKey(2), 2)
	e.add(iKey(3), 1)
	if e.used != 4 {
		t.Fatal("used not being incremented")
	}
	if e.keyCosts[iKey(2)] != 2 {
		t.Fatal("keyCosts not being updated")
	}
}

func TestSampledLFUDel(t *testing.T) {
	e := newSampledLFU(4)
	e.add(iKey(1), 1)
	e.add(iKey(2), 2)
	e.del(iKey(2))
	if e.used != 1 {
		t.Fatal("del not updating used field")
	}
	if _, ok := e.keyCosts[iKey(2)]; ok {
		t.Fatal("del not deleting value from keyCosts")
	}
	e.del(iKey(4))
}

func TestSampledLFUUpdate(t *testing.T) {
	e := newSampledLFU(4)
	e.add(iKey(1), 1)
	if !e.updateIfHas(iKey(1), 2) {
		t.Fatal("update should be possible")
	}
	if e.used != 2 {
		t.Fatal("update not changing used field")
	}
	if e.updateIfHas(iKey(2), 2) {
		t.Fatal("update shouldn't be possible")
	}
}

func TestSampledLFUClear(t *testing.T) {
	e := newSampledLFU(4)
	e.add(iKey(1), 1)
	e.add(iKey(2), 2)
	e.add(iKey(3), 1)
	e.clear()
	if len(e.keyCosts) != 0 || e.used != 0 {
		t.Fatal("clear not deleting keyCosts or zeroing used field")
	}
}

func TestSampledLFURoom(t *testing.T) {
	e := newSampledLFU(16)
	e.add(iKey(1), 1)
	e.add(iKey(2), 2)
	e.add(iKey(3), 3)
	if e.roomLeft(4) != 6 {
		t.Fatal("roomLeft returning wrong value")
	}
}

func TestSampledLFUSample(t *testing.T) {
	e := newSampledLFU(16)
	e.add(iKey(4), 4)
	e.add(iKey(5), 5)
	sample := e.fillSample([]*policyPair{
		{iKey(1), 1},
		{iKey(2), 2},
		{iKey(3), 3},
	})
	k := sample[len(sample)-1].key
	if len(sample) != 5 || k.ID == 1 || k.ID == 2 || k.ID == 3 {
		t.Fatal("fillSample not filling properly")
	}
	if len(sample) != len(e.fillSample(sample)) {
		t.Fatal("fillSample mutating full sample")
	}
	e.del(iKey(5))
	if sample = e.fillSample(sample[:len(sample)-2]); len(sample) != 4 {
		t.Fatal("fillSample not returning sample properly")
	}
}

func TestTinyLFUIncrement(t *testing.T) {
	a := newTinyLFU(4)
	a.Increment(iKey(1))
	a.Increment(iKey(1))
	a.Increment(iKey(1))
	if !a.door.Has(1) {
		t.Fatal("doorkeeper bit not set")
	}
	if a.freq.Estimate(1) != 2 {
		t.Fatal("incorrect counter value")
	}
	a.Increment(iKey(1))
	if a.door.Has(1) {
		t.Fatal("doorkeeper bit set after reset")
	}
	if a.freq.Estimate(1) != 1 {
		t.Fatal("counter value not halved after reset")
	}
}

func TestTinyLFUEstimate(t *testing.T) {
	a := newTinyLFU(8)
	a.Increment(iKey(1))
	a.Increment(iKey(1))
	a.Increment(iKey(1))
	if a.Estimate(iKey(1)) != 3 {
		t.Fatal("estimate value incorrect")
	}
	if a.Estimate(iKey(2)) != 0 {
		t.Fatal("estimate value should be 0")
	}
}

func TestTinyLFUPush(t *testing.T) {
	a := newTinyLFU(16)
	a.Push(makeKeys(1, 2, 2, 3, 3, 3))
	if a.Estimate(iKey(1)) != 1 || a.Estimate(iKey(2)) != 2 || a.Estimate(iKey(3)) != 3 {
		t.Fatal("push didn't increment counters properly")
	}
	if a.incrs != 6 {
		t.Fatal("incrs not being incremented")
	}
}

func TestTinyLFUClear(t *testing.T) {
	a := newTinyLFU(16)
	a.Push(makeKeys(1, 3, 3, 3))
	a.clear()
	if a.incrs != 0 || a.Estimate(iKey(3)) != 0 {
		t.Fatal("clear not clearing")
	}
}
