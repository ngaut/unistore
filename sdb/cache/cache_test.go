package cache

import (
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var wait time.Duration = time.Millisecond * 10

func TestCacheMaxCost(t *testing.T) {
	key := func() Key {
		return Key{ID: uint64(rand.Intn(36 * 36))}
	}
	c, err := NewCache(&Config{
		NumCounters: 12960, // 36^2 * 10
		MaxCost:     1e6,   // 1mb
		BufferItems: 64,
		Metrics:     true,
	})
	if err != nil {
		panic(err)
	}
	stop := make(chan struct{}, 8)
	for i := 0; i < 8; i++ {
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					time.Sleep(time.Millisecond)

					k := key()
					if _, ok := c.Get(k); !ok {
						val := ""
						if rand.Intn(100) < 10 {
							val = "test"
						} else {
							val = strings.Repeat("a", 1000)
						}
						c.Set(key(), val, int64(2+len(val)))
					}
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		time.Sleep(time.Second)
		cacheCost := c.Metrics.CostAdded() - c.Metrics.CostEvicted()
		t.Logf("total cache cost: %d\n", cacheCost)
		if float64(cacheCost) > float64(1e6*1.05) {
			t.Fatal("cache cost exceeding MaxCost")
		}
	}
	for i := 0; i < 8; i++ {
		stop <- struct{}{}
	}
}

func TestCache(t *testing.T) {
	if _, err := NewCache(&Config{
		NumCounters: 0,
	}); err == nil {
		t.Fatal("numCounters can't be 0")
	}
	if _, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     0,
	}); err == nil {
		t.Fatal("maxCost can't be 0")
	}
	if _, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 0,
	}); err == nil {
		t.Fatal("bufferItems can't be 0")
	}
	if c, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
		Metrics:     true,
	}); c == nil || err != nil {
		t.Fatal("config should be good")
	}
}

func TestCacheProcessItems(t *testing.T) {
	m := &sync.Mutex{}
	evicted := make(map[Key]struct{})
	c, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
		Cost: func(value interface{}) int64 {
			return int64(value.(int))
		},
		OnEvict: func(key Key, value interface{}) {
			m.Lock()
			defer m.Unlock()
			evicted[key] = struct{}{}
		},
	})
	if err != nil {
		panic(err)
	}

	c.Set(iKey(1), 1, 0)
	time.Sleep(wait)
	if !c.policy.Has(Key{ID: 1}) || c.policy.Cost(iKey(1)) != 1 {
		t.Fatal("cache processItems didn't add new item")
	}
	c.Set(iKey(1), 2, 0)
	time.Sleep(wait)
	if c.policy.Cost(iKey(1)) != 2 {
		t.Fatal("cache processItems didn't update item cost")
	}
	c.Del(iKey(1))
	time.Sleep(wait)
	if val, ok := c.store.Get(iKey(1)); val != nil || ok {
		t.Fatal("cache processItems didn't delete item")
	}
	if c.policy.Has(iKey(1)) {
		t.Fatal("cache processItems didn't delete item")
	}
	c.Set(iKey(2), 2, 3)
	c.Set(iKey(3), 3, 3)
	c.Set(iKey(5), 3, 3)
	c.Set(iKey(5), 3, 5)
	c.Set(iKey(1), 3, 3)
	time.Sleep(wait)
	m.Lock()
	if len(evicted) == 0 {
		m.Unlock()
		t.Fatal("cache processItems not evicting or calling OnEvict")
	}
	m.Unlock()
	c.Close()
}

func iKey(id int) Key {
	return Key{ID: uint64(id)}
}

func makeKeys(ids ...int) []Key {
	keys := make([]Key, len(ids))
	for i, id := range ids {
		keys[i] = iKey(id)
	}
	return keys
}

func TestCacheGet(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
		Metrics:     true,
	})
	if err != nil {
		panic(err)
	}
	c.Set(iKey(1), 1, 1)
	if val, ok := c.Get(iKey(1)); val == nil || !ok {
		t.Fatal("get should be successful")
	}
	if val, ok := c.Get(iKey(2)); val != nil || ok {
		t.Fatal("get should not be successful")
	}
	// 0.5 and not 1.0 because we tried Getting each item twice
	if c.Metrics.Ratio() != 0.5 {
		t.Fatal("get should record metrics")
	}
	c = nil
	if val, ok := c.Get(iKey(0)); val != nil || ok {
		t.Fatal("get should not be successful with nil cache")
	}
}

func TestCacheSet(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
		Metrics:     true,
	})
	if err != nil {
		panic(err)
	}
	c.Set(iKey(1), 1, 1)
	time.Sleep(wait)
	if val, ok := c.Get(iKey(1)); val == nil || val.(int) != 1 || !ok {
		t.Fatal("set/get returned wrong value")
	}

	c.Set(iKey(1), 2, 2)
	val, ok := c.store.GetValue(iKey(1))
	if val == nil || val.(int) != 2 || !ok {
		t.Fatal("set/update was unsuccessful")
	}
	c.stop <- struct{}{}
	for i := 0; i < setBufSize; i++ {
		c.Set(iKey(1), 1, 1)
	}
	close(c.setBuf)
	close(c.stop)
}

func TestCacheGetOrCompute(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
		Metrics:     true,
	})
	if err != nil {
		t.Fatal(err)
	}
	p := runtime.GOMAXPROCS(0)
	start := make(chan struct{})
	var done sync.WaitGroup
	var cnt uint32
	setFlags := make([]bool, p)
	results := make([]int, p)

	for n := 0; n < p; n++ {
		done.Add(1)
		go func(id int) {
			<-start
			var set bool
			var result interface{}
			for i := 0; i < 10; i++ {
				result, _ = c.GetOrCompute(iKey(0), func() (interface{}, int64, error) {
					set = true
					atomic.AddUint32(&cnt, 1)
					return id, 1, nil
				})
			}
			if set {
				setFlags[id] = true
			}
			results[id] = result.(int)
			done.Done()
		}(n)
	}
	close(start)
	done.Wait()

	if cnt != 1 {
		t.Fatalf("the factory function is called %d times", cnt)
	}
	setter := -1
	for id, flag := range setFlags {
		if !flag {
			continue
		}
		if setter != -1 {
			t.Fatal("more than one setter success")
		}
		setter = id
	}
	for id, v := range results {
		if v != setter {
			t.Fatalf("%d got different result (%d != %d)", id, v, setter)
		}
	}
}

func TestCacheDel(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
	})
	if err != nil {
		panic(err)
	}
	c.Set(iKey(1), 1, 1)
	c.Del(iKey(1))
	time.Sleep(wait)
	if val, ok := c.Get(iKey(1)); val != nil || ok {
		t.Fatal("del didn't delete")
	}
	c = nil
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("del panic with nil cache")
		}
	}()
	c.Del(iKey(1))
}

func TestCacheClear(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
		Metrics:     true,
	})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 10; i++ {
		c.Set(iKey(i), i, 1)
	}
	time.Sleep(wait)
	if c.Metrics.KeysAdded() != 10 {
		t.Fatal("range of sets not being processed")
	}
	c.Clear()
	if c.Metrics.KeysAdded() != 0 {
		t.Fatal("clear didn't reset metrics")
	}
	for i := 0; i < 10; i++ {
		if val, ok := c.Get(iKey(i)); val != nil || ok {
			t.Fatal("clear didn't delete values")
		}
	}
}

func TestCacheMetrics(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
		Metrics:     true,
	})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 10; i++ {
		c.Set(iKey(i), i, 1)
	}
	time.Sleep(wait)
	m := c.Metrics
	if m.KeysAdded() != 10 {
		t.Fatal("metrics exporting incorrect fields")
	}
}

func TestMetrics(t *testing.T) {
	newMetrics()
}

func TestMetricsAddGet(t *testing.T) {
	m := newMetrics()
	m.add(hit, 1, 1)
	m.add(hit, 2, 2)
	m.add(hit, 3, 3)
	if m.Hits() != 6 {
		t.Fatal("add/get error")
	}
	m = nil
	m.add(hit, 1, 1)
	if m.Hits() != 0 {
		t.Fatal("get with nil struct should return 0")
	}
}

func TestMetricsRatio(t *testing.T) {
	m := newMetrics()
	if m.Ratio() != 0 {
		t.Fatal("ratio with no hits or misses should be 0")
	}
	m.add(hit, 1, 1)
	m.add(hit, 2, 2)
	m.add(miss, 1, 1)
	m.add(miss, 2, 2)
	if m.Ratio() != 0.5 {
		t.Fatal("ratio incorrect")
	}
	m = nil
	if m.Ratio() != 0.0 {
		t.Fatal("ratio with a nil struct should return 0")
	}
}

func TestMetricsString(t *testing.T) {
	m := newMetrics()
	m.add(hit, 1, 1)
	m.add(miss, 1, 1)
	m.add(keyAdd, 1, 1)
	m.add(keyUpdate, 1, 1)
	m.add(keyEvict, 1, 1)
	m.add(costAdd, 1, 1)
	m.add(costEvict, 1, 1)
	m.add(dropSets, 1, 1)
	m.add(rejectSets, 1, 1)
	m.add(dropGets, 1, 1)
	m.add(keepGets, 1, 1)
	if m.Hits() != 1 || m.Misses() != 1 || m.Ratio() != 0.5 || m.KeysAdded() != 1 ||
		m.KeysUpdated() != 1 || m.KeysEvicted() != 1 || m.CostAdded() != 1 ||
		m.CostEvicted() != 1 || m.SetsDropped() != 1 || m.SetsRejected() != 1 ||
		m.GetsDropped() != 1 || m.GetsKept() != 1 {
		t.Fatal("Metrics wrong value(s)")
	}
	if len(m.String()) == 0 {
		t.Fatal("Metrics.String() empty")
	}
	m = nil
	if len(m.String()) != 0 {
		t.Fatal("Metrics.String() should be empty with nil struct")
	}
	if stringFor(doNotUse) != "unidentified" {
		t.Fatal("stringFor() not handling doNotUse type")
	}
}

func TestCacheMetricsClear(t *testing.T) {
	c, err := NewCache(&Config{
		NumCounters: 100,
		MaxCost:     10,
		BufferItems: 64,
		Metrics:     true,
	})
	if err != nil {
		panic(err)
	}
	c.Set(iKey(1), 1, 1)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				c.Get(iKey(1))
			}
		}
	}()
	time.Sleep(wait)
	c.Clear()
	stop <- struct{}{}
	c.Metrics = nil
	c.Metrics.Clear()
}
