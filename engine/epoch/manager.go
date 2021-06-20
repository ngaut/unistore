// Copyright 2019-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package epoch

import (
	"time"
	"unsafe"

	"github.com/pingcap/badger/y"
)

type Guard struct {
	localEpoch atomicEpoch
	mgr        *ResourceManager
	deletions  []deletion

	next unsafe.Pointer
}

func (g *Guard) Delete(resources []Resource) {
	globalEpoch := g.mgr.currentEpoch.load()
	g.deletions = append(g.deletions, deletion{
		epoch:     globalEpoch,
		resources: resources,
	})
}

func (g *Guard) Done() {
	g.localEpoch.store(g.localEpoch.load().deactivate())
}

func (g *Guard) collect(globalEpoch epoch) bool {
	ds := g.deletions[:0]
	for _, d := range g.deletions {
		if globalEpoch.sub(d.epoch) < 2 {
			ds = append(ds, d)
			continue
		}
		for _, r := range d.resources {
			r.Delete()
		}
		d.resources = nil
	}
	g.deletions = ds
	return len(ds) == 0
}

type Resource interface {
	Delete() error
}

type ResourceManager struct {
	currentEpoch atomicEpoch

	// TODO: cache line size for non x86
	// cachePad make currentEpoch stay in a separate cache line.
	cachePad [64]byte
	guards   guardList
}

func NewResourceManager(c *y.Closer) *ResourceManager {
	rm := &ResourceManager{
		currentEpoch: atomicEpoch{epoch: 1 << 1},
	}
	c.AddRunning(1)
	go rm.collectLoop(c)
	return rm
}

func (rm *ResourceManager) Acquire() *Guard {
	g := &Guard{
		mgr: rm,
	}
	g.localEpoch.store(rm.currentEpoch.load().activate())
	rm.guards.add(g)
	return g
}

func (rm *ResourceManager) collectLoop(c *y.Closer) {
	defer c.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			rm.collect()
		case <-c.HasBeenClosed():
			return
		}
	}
}

func (rm *ResourceManager) collect() {
	canAdvance := true
	globalEpoch := rm.currentEpoch.load()

	rm.guards.iterate(func(guard *Guard) bool {
		localEpoch := guard.localEpoch.load()

		if localEpoch.isActive() {
			canAdvance = canAdvance && localEpoch.sub(globalEpoch) == 0
			return false
		}

		return guard.collect(globalEpoch)
	})

	if canAdvance {
		rm.currentEpoch.store(globalEpoch.successor())
	}
}

type deletion struct {
	epoch     epoch
	resources []Resource
}
