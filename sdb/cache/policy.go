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

package cache

import (
	"math"
	"sync"

	"github.com/ngaut/unistore/sdb/cache/z"
)

const (
	// lfuSample is the number of items to sample when looking at eviction
	// candidates. 5 seems to be the most optimal number [citation needed].
	lfuSample = 5
)

type policy struct {
	sync.Mutex
	admit   *tinyLFU
	evict   *sampledLFU
	itemsCh chan []uint64
	stop    chan struct{}
	metrics *Metrics
}

func newPolicy(numCounters, maxCost int64) *policy {
	p := &policy{
		admit:   newTinyLFU(numCounters),
		evict:   newSampledLFU(maxCost),
		itemsCh: make(chan []uint64, 3),
		stop:    make(chan struct{}),
	}
	go p.processItems()
	return p
}

func (p *policy) CollectMetrics(metrics *Metrics) {
	p.metrics = metrics
	p.evict.metrics = metrics
}

type policyPair struct {
	key  uint64
	cost int64
}

func (p *policy) processItems() {
	for {
		select {
		case items := <-p.itemsCh:
			p.Lock()
			p.admit.Push(items)
			p.Unlock()
		case <-p.stop:
			return
		}
	}
}

func (p *policy) setNewMaxCost(newMaxCost int64) {
	p.Lock()
	p.evict.maxCost = newMaxCost
	p.Unlock()
}

func (p *policy) Push(keys []uint64) bool {
	if len(keys) == 0 {
		return true
	}
	select {
	case p.itemsCh <- keys:
		p.metrics.add(keepGets, keys[0], uint64(len(keys)))
		return true
	default:
		p.metrics.add(dropGets, keys[0], uint64(len(keys)))
		return false
	}
}

func (p *policy) Add(key uint64, cost int64) ([]*item, bool) {
	p.Lock()
	defer p.Unlock()
	// can't add an item bigger than entire cache
	if cost > p.evict.maxCost {
		return nil, false
	}
	// we don't need to go any further if the item is already in the cache
	if has := p.evict.updateIfHas(key, cost); has {
		return nil, true
	}
	// if we got this far, this key doesn't exist in the cache
	//
	// calculate the remaining room in the cache (usually bytes)
	room := p.evict.roomLeft(cost)
	if room >= 0 {
		// there's enough room in the cache to store the new item without
		// overflowing, so we can do that now and stop here
		p.evict.add(key, cost)
		p.metrics.add(costAdd, key, uint64(cost))
		p.metrics.add(keyAdd, key, 1)
		return nil, true
	}
	// incHits is the hit count for the incoming item
	incHits := p.admit.Estimate(key)
	// sample is the eviction candidate pool to be filled via random sampling
	//
	// TODO: perhaps we should use a min heap here. Right now our time
	// complexity is N for finding the min. Min heap should bring it down to
	// O(lg N).
	sample := make([]*policyPair, 0, lfuSample)
	// as items are evicted they will be appended to victims
	victims := make([]*item, 0)
	// delete victims until there's enough space or a minKey is found that has
	// more hits than incoming item.
	for ; room < 0; room = p.evict.roomLeft(cost) {
		// fill up empty slots in sample
		sample = p.evict.fillSample(sample)
		// find minimally used item in sample
		minKey, minHits, minId := uint64(0), int64(math.MaxInt64), 0
		for i, pair := range sample {
			// look up hit count for sample key
			if hits := p.admit.Estimate(pair.key); hits < minHits {
				minKey, minHits, minId = pair.key, hits, i
			}
		}
		// if the incoming item isn't worth keeping in the policy, reject.
		if incHits < minHits {
			p.metrics.add(rejectSets, key, 1)
			return victims, false
		}
		// delete the victim from metadata
		p.evict.del(minKey)

		// delete the victim from sample
		sample[minId] = sample[len(sample)-1]
		sample = sample[:len(sample)-1]
		// store victim in evicted victims slice
		victims = append(victims, &item{
			key: minKey,
		})
	}
	p.evict.add(key, cost)
	p.metrics.add(costAdd, key, uint64(cost))
	p.metrics.add(keyAdd, key, 1)
	return victims, true
}

func (p *policy) Has(key uint64) bool {
	p.Lock()
	_, exists := p.evict.keyCosts[key]
	p.Unlock()
	return exists
}

func (p *policy) Del(key uint64) {
	p.Lock()
	p.evict.del(key)
	p.Unlock()
}

func (p *policy) Cap() int64 {
	p.Lock()
	capacity := int64(p.evict.maxCost - p.evict.used)
	p.Unlock()
	return capacity
}

func (p *policy) Update(key uint64, cost int64) {
	p.Lock()
	p.evict.updateIfHas(key, cost)
	p.Unlock()
}

func (p *policy) Cost(key uint64) int64 {
	p.Lock()
	if cost, found := p.evict.keyCosts[key]; found {
		p.Unlock()
		return cost
	}
	p.Unlock()
	return -1
}

func (p *policy) Clear() {
	p.Lock()
	p.admit.clear()
	p.evict.clear()
	p.Unlock()
}

func (p *policy) Close() {
	// block until p.processItems goroutine is returned
	p.stop <- struct{}{}
	close(p.stop)
	close(p.itemsCh)
}

// sampledLFU is an eviction helper storing key-cost pairs.
type sampledLFU struct {
	keyCosts map[uint64]int64
	maxCost  int64
	used     int64
	metrics  *Metrics
}

func newSampledLFU(maxCost int64) *sampledLFU {
	return &sampledLFU{
		keyCosts: make(map[uint64]int64),
		maxCost:  maxCost,
	}
}

func (p *sampledLFU) roomLeft(cost int64) int64 {
	return p.maxCost - (p.used + cost)
}

func (p *sampledLFU) fillSample(in []*policyPair) []*policyPair {
	if len(in) >= lfuSample {
		return in
	}
	for key, cost := range p.keyCosts {
		in = append(in, &policyPair{key, cost})
		if len(in) >= lfuSample {
			return in
		}
	}
	return in
}

func (p *sampledLFU) del(key uint64) {
	cost, ok := p.keyCosts[key]
	if !ok {
		return
	}
	p.used -= cost
	delete(p.keyCosts, key)
	p.metrics.add(costEvict, key, uint64(cost))
	p.metrics.add(keyEvict, key, 1)
}

func (p *sampledLFU) add(key uint64, cost int64) {
	p.keyCosts[key] = cost
	p.used += cost
}

func (p *sampledLFU) updateIfHas(key uint64, cost int64) bool {
	if prev, found := p.keyCosts[key]; found {
		// update the cost of an existing key, but don't worry about evicting,
		// evictions will be handled the next time a new item is added
		p.metrics.add(keyUpdate, key, 1)
		if prev > cost {
			diff := prev - cost
			p.metrics.add(costAdd, key, ^uint64(uint64(diff)-1))
		} else if cost > prev {
			diff := cost - prev
			p.metrics.add(costAdd, key, uint64(diff))
		}
		p.used += cost - prev
		p.keyCosts[key] = cost
		return true
	}
	return false
}

func (p *sampledLFU) clear() {
	p.used = 0
	p.keyCosts = make(map[uint64]int64)
}

// tinyLFU is an admission helper that keeps track of access frequency using
// tiny (4-bit) counters in the form of a count-min sketch.
// tinyLFU is NOT thread safe.
type tinyLFU struct {
	freq    *cmSketch
	door    *z.Bloom
	incrs   int64
	resetAt int64
}

func newTinyLFU(numCounters int64) *tinyLFU {
	return &tinyLFU{
		freq:    newCmSketch(numCounters),
		door:    z.NewBloomFilter(float64(numCounters), 0.01),
		resetAt: numCounters,
	}
}

func (p *tinyLFU) Push(keys []uint64) {
	for _, key := range keys {
		p.Increment(key)
	}
}

func (p *tinyLFU) Estimate(key uint64) int64 {
	hits := p.freq.Estimate(key)
	if p.door.Has(key) {
		hits += 1
	}
	return hits
}

func (p *tinyLFU) Increment(key uint64) {
	// flip doorkeeper bit if not already
	if added := p.door.AddIfNotHas(key); !added {
		// increment count-min counter if doorkeeper bit is already set.
		p.freq.Increment(key)
	}
	p.incrs++
	if p.incrs >= p.resetAt {
		p.reset()
	}
}

func (p *tinyLFU) reset() {
	// Zero out incrs.
	p.incrs = 0
	// clears doorkeeper bits
	p.door.Clear()
	// halves count-min counters
	p.freq.Reset()
}

func (p *tinyLFU) clear() {
	p.incrs = 0
	p.door.Clear()
	p.freq.Clear()
}
