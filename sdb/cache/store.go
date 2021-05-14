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
	"sync"
	"sync/atomic"
	"unsafe"
)

const numShards = 256

// item is passed to setBuf so items can eventually be added to the cache
type item struct {
	sync.Mutex
	dead  bool
	key   uint64
	value atomic.Value
}

func (i *item) del(s *store) bool {
	if i.dead {
		return false
	}

	i.dead = true
	s.Del(i.key)
	return true
}

// TODO: implement a more powerful concurrent hash, such as cuckoo hash.
type shardMap struct {
	sync.RWMutex
	data map[uint64]*item
}

type shard struct {
	shardMap
	pad [64 - unsafe.Sizeof(shardMap{})%64]byte
}

type store struct {
	shards [numShards]shard
}

func newStore() *store {
	s := new(store)
	for i := range s.shards {
		s.shards[i].data = make(map[uint64]*item)
	}
	return s
}

func (s *store) GetValue(key uint64) (interface{}, bool) {
	i, ok := s.Get(key)
	if !ok {
		return nil, false
	}
	return i.value.Load(), true
}

func (s *store) Get(key uint64) (*item, bool) {
	m := &s.shards[key%numShards]
	m.RLock()
	i, ok := m.data[key]
	m.RUnlock()
	return i, ok
}

func (s *store) GetOrNew(key uint64) *item {
	m := &s.shards[key%numShards]
	m.RLock()
	if i, ok := m.data[key]; ok {
		m.RUnlock()
		return i
	}
	m.RUnlock()
	m.Lock()
	if i, ok := m.data[key]; ok {
		m.Unlock()
		return i
	}
	i := &item{key: key}
	m.data[key] = i
	m.Unlock()
	return i
}

func (s *store) Set(key uint64, value *item) {
}

func (s *store) Del(key uint64) *item {
	m := &s.shards[key%numShards]
	m.Lock()
	i := m.data[key]
	delete(m.data, key)
	m.Unlock()
	return i
}

func (s *store) Clear() {
	for i := uint64(0); i < numShards; i++ {
		s.shards[i].Lock()
		s.shards[i].data = make(map[uint64]*item)
		s.shards[i].Unlock()
	}
}
