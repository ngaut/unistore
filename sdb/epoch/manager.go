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
	payload    interface{}

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

func (rm *ResourceManager) AcquireWithPayload(payload interface{}) *Guard {
	g := &Guard{
		mgr:     rm,
		payload: payload,
	}
	g.localEpoch.store(rm.currentEpoch.load().activate())
	rm.guards.add(g)
	return g
}

func (rm *ResourceManager) Acquire() *Guard {
	return rm.AcquireWithPayload(nil)
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
