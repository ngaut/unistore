/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Ristretto is a fast, fixed size, in-memory cache with a dual focus on
// throughput and hit ratio performance. You can easily add Ristretto to an
// existing system and keep the most valuable data where you need it.
package cache

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
)

// Config is passed to NewCache for creating new Cache instances.
type Config struct {
	// NumCounters determines the number of counters (keys) to keep that hold
	// access frequency information. It's generally a good idea to have more
	// counters than the max cache capacity, as this will improve eviction
	// accuracy and subsequent hit ratios.
	//
	// For example, if you expect your cache to hold 1,000,000 items when full,
	// NumCounters should be 10,000,000 (10x). Each counter takes up 4 bits, so
	// keeping 10,000,000 counters would require 5MB of memory.
	NumCounters int64
	// MaxCost can be considered as the cache capacity, in whatever units you
	// choose to use.
	//
	// For example, if you want the cache to have a max capacity of 100MB, you
	// would set MaxCost to 100,000,000 and pass an item's number of bytes as
	// the `cost` parameter for calls to Set. If new items are accepted, the
	// eviction process will take care of making room for the new item and not
	// overflowing the MaxCost value.
	MaxCost int64
	// BufferItems determines the size of Get buffers.
	//
	// Unless you have a rare use case, using `64` as the BufferItems value
	// results in good performance.
	BufferItems int64
	// Metrics determines whether cache statistics are kept during the cache's
	// lifetime. There *is* some overhead to keeping statistics, so you should
	// only set this flag to true when testing or throughput performance isn't a
	// major factor.
	Metrics bool
	// OnEvict is called for every eviction and passes the hashed key and
	// value to the function.
	OnEvict func(key uint64, value interface{})
	// Cost evaluates a value and outputs a corresponding cost. This function
	// is ran after Set is called for a new item or an item update with a cost
	// param of 0.
	Cost func(value interface{}) int64
}

const (
	// TODO: find the optimal value for this or make it configurable
	setBufSize = 32 * 1024
)

type setEvent struct {
	del  bool
	key  uint64
	cost int64
}

// Cache is a thread-safe implementation of a hashmap with a TinyLFU admission
// policy and a Sampled LFU eviction policy. You can use the same Cache instance
// from as many goroutines as you want.
type Cache struct {
	// store is the central concurrent hashmap where key-value items are stored
	store *store
	// policy determines what gets let in to the cache and what gets kicked out
	policy *policy
	// getBuf is a custom ring buffer implementation that gets pushed to when
	// keys are read
	getBuf *ringBuffer
	// setBuf is a buffer allowing us to batch/drop Sets during times of high
	// contention
	setBuf chan setEvent
	// onEvict is called for item evictions
	onEvict func(uint64, interface{})
	// stop is used to stop the processItems goroutine
	stop chan struct{}
	// cost calculates cost from a value
	cost func(value interface{}) int64
	// Metrics contains a running log of important statistics like hits, misses,
	// and dropped items
	Metrics *Metrics
}

// NewCache returns a new Cache instance and any configuration errors, if any.
func NewCache(config *Config) (*Cache, error) {
	switch {
	case config.NumCounters == 0:
		return nil, errors.New("NumCounters can't be zero.")
	case config.MaxCost == 0:
		return nil, errors.New("MaxCost can't be zero.")
	case config.BufferItems == 0:
		return nil, errors.New("BufferItems can't be zero.")
	}
	policy := newPolicy(config.NumCounters, config.MaxCost)
	cache := &Cache{
		store:   newStore(),
		policy:  policy,
		getBuf:  newRingBuffer(policy, config.BufferItems),
		setBuf:  make(chan setEvent, setBufSize),
		onEvict: config.OnEvict,
		stop:    make(chan struct{}),
		cost:    config.Cost,
	}
	if config.Metrics {
		cache.collectMetrics()
	}
	// NOTE: benchmarks seem to show that performance decreases the more
	//       goroutines we have running cache.processItems(), so 1 should
	//       usually be sufficient
	go cache.processItems()
	return cache, nil
}

// Get returns the value (if any) and a boolean representing whether the
// value was found or not. The value can be nil and the boolean can be true at
// the same time.
func (c *Cache) Get(key uint64) (interface{}, bool) {
	if c == nil {
		return nil, false
	}
	c.getBuf.Push(key)
	value, ok := c.store.GetValue(key)
	if ok {
		c.Metrics.add(hit, key, 1)
	} else {
		c.Metrics.add(miss, key, 1)
	}
	return value, ok
}

// Set attempts to add the key-value item to the cache. There's still a chance it
// could be dropped by the policy if its determined that the key-value item
// isn't worth keeping, but otherwise the item will be added and other items
// will be evicted in order to make room.
//
// To dynamically evaluate the items cost using the Config.Coster function, set
// the cost parameter to 0 and Coster will be ran when needed in order to find
// the items true cost.
func (c *Cache) Set(key uint64, value interface{}, cost int64) {
	if c == nil {
		return
	}
	if cost == 0 && c.cost != nil {
		cost = c.cost(value)
	}
	for {
		i := c.store.GetOrNew(key)
		i.Lock()
		if i.dead {
			i.Unlock()
			continue
		}

		i.value.Store(value)
		// Send event to channel while holding mutex, so we can keep the order of event log respect to
		// the order of mutations on hash map. If we send envent after i.Unlock() we may have danling item in hash map:
		// 	* Mutations is `A delete k -> B insert K`
		//	* But the event log is `B insert K -> A delete K`
		//	* After apply the event log, there are danling item in hash map which cannot be evicted.
		// Delete item when apply delete event is not a good idea, because we may overwrite the following insert.
		// Delay all hash map mutations to log replay seems a good idea, but it will result in a confusing behavior,
		// that is you cannot get the item you just inserted.
		c.setBuf <- setEvent{del: false, key: key, cost: cost}

		i.Unlock()
		return
	}
}

// SetNewMaxCost set maxCost to newMaxCost
func (c *Cache) SetNewMaxCost(newMaxCost int64) {
	c.policy.setNewMaxCost(newMaxCost)
}

// GetOrCompute returns the value of key. If there is no such key, it will compute the
// value using the factory function `f`. If there are concurrent call on same key,
// the factory function will be called only once.
func (c *Cache) GetOrCompute(key uint64, f func() (interface{}, int64, error)) (interface{}, error) {
	if c == nil {
		return nil, nil
	}
	for {
		i := c.store.GetOrNew(key)
		if v := i.value.Load(); v != nil {
			return v, nil
		}
		if v, err, ok := c.compute(i, f); ok {
			return v, err
		}
	}
}

func (c *Cache) compute(i *item, f func() (interface{}, int64, error)) (interface{}, error, bool) {
	i.Lock()
	defer i.Unlock()
	if i.dead {
		return nil, nil, false
	}

	// Double check.
	if v := i.value.Load(); v != nil {
		return v, nil, true
	}

	v, cost, err := f()
	if err != nil {
		return nil, err, true
	}
	i.value.Store(v)

	if cost == 0 && c.cost != nil {
		cost = c.cost(v)
	}
	c.setBuf <- setEvent{del: false, key: i.key, cost: cost}
	return v, nil, true
}

// Del deletes the key-value item from the cache if it exists.
func (c *Cache) Del(key uint64) interface{} {
	if c == nil {
		return nil
	}
	i, ok := c.store.Get(key)
	if !ok {
		return nil
	}
	i.Lock()
	if i.del(c.store) {
		c.setBuf <- setEvent{del: true, key: key}
		c.store.Del(key)
	}
	v := i.value.Load()
	i.Unlock()
	return v
}

// Close stops all goroutines and closes all channels.
func (c *Cache) Close() {
	// block until processItems goroutine is returned
	c.stop <- struct{}{}
	close(c.stop)

	// TODO: Close will be called when shutdown DB, but some table will try to
	// evict data from cache in epoch's background thread, if we close setBuf here
	// runtime will panic.
	//
	// It is safe to let this channel keeps open, because the DB process is going to terminate.
	//
	// To address this issue, we must wait epoch manger to close before close cache.
	// For now just ignore this channel.

	// close(c.setBuf)

	c.policy.Close()
}

// IsEmpty check setBuf is empty, if it is not empty, pop a buf.
func IsEmpty(ch <-chan setEvent) bool {
	select {
	case <-ch:
		return false
	default:
		return true
	}
}

// Clear empties the hashmap and zeroes all policy counters. Note that this is
// not an atomic operation (but that shouldn't be a problem as it's assumed that
// Set/Get calls won't be occurring until after this).
func (c *Cache) Clear() {
	// block until processItems goroutine is returned
	c.stop <- struct{}{}
	// Empty the setBuf
	// there should not be any Set or Get when invoke Cache.Clear().
	for !IsEmpty(c.setBuf) {
	}
	// clear value hashmap and policy data
	c.policy.Clear()
	c.store.Clear()
	// only reset metrics if they're enabled
	if c.Metrics != nil {
		c.Metrics.Clear()
	}
	// restart processItems goroutine
	go c.processItems()
}

// processItems is ran by goroutines processing the Set buffer.
func (c *Cache) processItems() {
	for {
		select {
		case e := <-c.setBuf:
			if e.del {
				c.policy.Del(e.key)
				continue
			}
			c.handleNewItem(e.key, e.cost)
		case <-c.stop:
			return
		}
	}
}

func (c *Cache) handleNewItem(key uint64, cost int64) {
	itemInMap, ok := c.store.Get(key)
	if !ok {
		// This item dropped by admission policy,
		// ignore this event or we may have danling item in policy.
		return
	}

	// TODO: do evict after all events in current batch handled.
	victims, added := c.policy.Add(key, cost)
	if !added {
		// Item dropped by admission policy, delete it from hash map.
		// Otherwise this danling item will be kept in cache forever.
		i, ok := c.store.Get(key)
		if !ok || i != itemInMap {
			return
		}
		i.Lock()
		deleted := i.del(c.store)
		i.Unlock()

		if deleted && c.onEvict != nil {
			c.onEvict(i.key, i.value)
		}
		return
	}

	for _, victim := range victims {
		victim, ok = c.store.Get(victim.key)
		if !ok {
			continue
		}
		victim.Lock()
		deleted := victim.del(c.store)
		victim.Unlock()
		if deleted && c.onEvict != nil {
			c.onEvict(victim.key, victim.value.Load())
		}
	}
}

// collectMetrics just creates a new *Metrics instance and adds the pointers
// to the cache and policy instances.
func (c *Cache) collectMetrics() {
	c.Metrics = newMetrics()
	c.policy.CollectMetrics(c.Metrics)
}

type metricType int

const (
	// The following 2 keep track of hits and misses.
	hit = iota
	miss
	// The following 3 keep track of number of keys added, updated and evicted.
	keyAdd
	keyUpdate
	keyEvict
	// The following 2 keep track of cost of keys added and evicted.
	costAdd
	costEvict
	// The following keep track of how many sets were dropped or rejected later.
	dropSets
	rejectSets
	// The following 2 keep track of how many gets were kept and dropped on the
	// floor.
	dropGets
	keepGets
	// This should be the final enum. Other enums should be set before this.
	doNotUse
)

func stringFor(t metricType) string {
	switch t {
	case hit:
		return "hit"
	case miss:
		return "miss"
	case keyAdd:
		return "keys-added"
	case keyUpdate:
		return "keys-updated"
	case keyEvict:
		return "keys-evicted"
	case costAdd:
		return "cost-added"
	case costEvict:
		return "cost-evicted"
	case dropSets:
		return "sets-dropped"
	case rejectSets:
		return "sets-rejected" // by policy.
	case dropGets:
		return "gets-dropped"
	case keepGets:
		return "gets-kept"
	default:
		return "unidentified"
	}
}

// Metrics is a snapshot of performance statistics for the lifetime of a cache
// instance.
type Metrics struct {
	all [doNotUse][]*uint64
}

func newMetrics() *Metrics {
	s := &Metrics{}
	for i := 0; i < doNotUse; i++ {
		s.all[i] = make([]*uint64, 256)
		slice := s.all[i]
		for j := range slice {
			slice[j] = new(uint64)
		}
	}
	return s
}

func (p *Metrics) add(t metricType, hash, delta uint64) {
	if p == nil {
		return
	}
	valp := p.all[t]
	// Avoid false sharing by padding at least 64 bytes of space between two
	// atomic counters which would be incremented.
	idx := (hash % 25) * 10
	atomic.AddUint64(valp[idx], delta)
}

func (p *Metrics) get(t metricType) uint64 {
	if p == nil {
		return 0
	}
	valp := p.all[t]
	var total uint64
	for i := range valp {
		total += atomic.LoadUint64(valp[i])
	}
	return total
}

// Hits is the number of Get calls where a value was found for the corresponding
// key.
func (p *Metrics) Hits() uint64 {
	return p.get(hit)
}

// Misses is the number of Get calls where a value was not found for the
// corresponding key.
func (p *Metrics) Misses() uint64 {
	return p.get(miss)
}

// KeysAdded is the total number of Set calls where a new key-value item was
// added.
func (p *Metrics) KeysAdded() uint64 {
	return p.get(keyAdd)
}

// KeysUpdated is the total number of Set calls where the value was updated.
func (p *Metrics) KeysUpdated() uint64 {
	return p.get(keyUpdate)
}

// KeysEvicted is the total number of keys evicted.
func (p *Metrics) KeysEvicted() uint64 {
	return p.get(keyEvict)
}

// CostAdded is the sum of costs that have been added (successful Set calls).
func (p *Metrics) CostAdded() uint64 {
	return p.get(costAdd)
}

// CostEvicted is the sum of all costs that have been evicted.
func (p *Metrics) CostEvicted() uint64 {
	return p.get(costEvict)
}

// SetsDropped is the number of Set calls that don't make it into internal
// buffers (due to contention or some other reason).
func (p *Metrics) SetsDropped() uint64 {
	return p.get(dropSets)
}

// SetsRejected is the number of Set calls rejected by the policy (TinyLFU).
func (p *Metrics) SetsRejected() uint64 {
	return p.get(rejectSets)
}

// GetsDropped is the number of Get counter increments that are dropped
// internally.
func (p *Metrics) GetsDropped() uint64 {
	return p.get(dropGets)
}

// GetsKept is the number of Get counter increments that are kept.
func (p *Metrics) GetsKept() uint64 {
	return p.get(keepGets)
}

// Ratio is the number of Hits over all accesses (Hits + Misses). This is the
// percentage of successful Get calls.
func (p *Metrics) Ratio() float64 {
	if p == nil {
		return 0.0
	}
	hits, misses := p.get(hit), p.get(miss)
	if hits == 0 && misses == 0 {
		return 0.0
	}
	return float64(hits) / float64(hits+misses)
}

func (p *Metrics) Clear() {
	if p == nil {
		return
	}
	for i := 0; i < doNotUse; i++ {
		for j := range p.all[i] {
			atomic.StoreUint64(p.all[i][j], 0)
		}
	}
}

func (p *Metrics) String() string {
	if p == nil {
		return ""
	}
	var buf bytes.Buffer
	for i := 0; i < doNotUse; i++ {
		t := metricType(i)
		fmt.Fprintf(&buf, "%s: %d ", stringFor(t), p.get(t))
	}
	fmt.Fprintf(&buf, "gets-total: %d ", p.get(hit)+p.get(miss))
	fmt.Fprintf(&buf, "hit-ratio: %.2f", p.Ratio())
	return buf.String()
}
