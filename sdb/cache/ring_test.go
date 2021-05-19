package cache

import (
	"sync"
	"testing"
)

type testConsumer struct {
	push func([]Key)
	save bool
}

func (c *testConsumer) Push(items []Key) bool {
	if c.save {
		c.push(items)
		return true
	}
	return false
}

func TestRingDrain(t *testing.T) {
	drains := 0
	r := newRingBuffer(&testConsumer{
		push: func(items []Key) {
			drains++
		},
		save: true,
	}, 1)
	for i := 0; i < 100; i++ {
		r.Push(iKey(i))
	}
	if drains != 100 {
		t.Fatal("buffers shouldn't be dropped with BufferItems == 1")
	}
}

func TestRingReset(t *testing.T) {
	drains := 0
	r := newRingBuffer(&testConsumer{
		push: func(items []Key) {
			drains++
		},
		save: false,
	}, 4)
	for i := 0; i < 100; i++ {
		r.Push(iKey(i))
	}
	if drains != 0 {
		t.Fatal("testConsumer shouldn't be draining")
	}
}

func TestRingConsumer(t *testing.T) {
	mu := &sync.Mutex{}
	drainItems := make(map[Key]struct{})
	r := newRingBuffer(&testConsumer{
		push: func(items []Key) {
			mu.Lock()
			defer mu.Unlock()
			for i := range items {
				drainItems[items[i]] = struct{}{}
			}
		},
		save: true,
	}, 4)
	for i := 0; i < 100; i++ {
		r.Push(iKey(i))
	}
	l := len(drainItems)
	if l == 0 || l > 100 {
		t.Fatal("drains not being processed correctly")
	}
}
