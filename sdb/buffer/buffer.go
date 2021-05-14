// Package buffer implements a variable-sized bytes pool.
package buffer

import (
	"sync"
)

const (
	numBuckets = 256
	pageSize   = 1024
)

// Buffers contains buckets for sharding.
type Buffers struct {
	pageSize int
	buckets  [numBuckets]bucket
}

type bucket struct {
	lock  sync.RWMutex
	pools map[int]*Pool
}

// Pool represents a fixed size bytes pool.
type Pool struct {
	size int
	pool *sync.Pool
}

// GetBuffer returns a bytes from the pool with the given size.
func (p *Pool) GetBuffer(size int) []byte {
	return p.pool.Get().([]byte)[:size]
}

// PutBuffer frees the bytes to the pool.
func (p *Pool) PutBuffer(buf []byte) {
	buf = buf[:cap(buf)]
	if cap(buf) >= p.size {
		p.pool.Put(buf)
	}
}

// NewBuffers creates a new Buffers with the given page size.
func NewBuffers(pageSize int) *Buffers {
	b := new(Buffers)
	for i := range b.buckets {
		b.buckets[i].pools = make(map[int]*Pool)
	}
	if pageSize < 1 {
		b.pageSize = 1
	} else {
		b.pageSize = pageSize
	}
	return b
}

// AssignPool assigns a fixed size bytes pool with the given size.
func (b *Buffers) AssignPool(size int) (p *Pool) {
	var alignedSize = size
	if size%b.pageSize > 0 {
		alignedSize = size/b.pageSize*b.pageSize + b.pageSize
	}
	m := &b.buckets[alignedSize/b.pageSize%numBuckets]
	var ok bool
	m.lock.RLock()
	if p, ok = m.pools[alignedSize]; ok {
		m.lock.RUnlock()
		return
	}
	m.lock.RUnlock()
	m.lock.Lock()
	if p, ok = m.pools[alignedSize]; !ok {
		p = &Pool{
			pool: &sync.Pool{New: func() interface{} {
				return make([]byte, alignedSize)
			}},
			size: alignedSize,
		}
		m.pools[alignedSize] = p
	}
	m.lock.Unlock()
	return
}

// GetBuffer returns a bytes from the pool with the given size.
func (b *Buffers) GetBuffer(size int) []byte {
	return b.AssignPool(size).GetBuffer(size)
}

// PutBuffer frees the bytes to the pool.
func (b *Buffers) PutBuffer(buf []byte) {
	b.AssignPool(cap(buf)).PutBuffer(buf)
}

// defaultBuffers is the default instance of *Buffers.
var defaultBuffers = NewBuffers(pageSize)

// AssignPool assigns a fixed size bytes pool with the given size.
func AssignPool(size int) *Pool {
	return defaultBuffers.AssignPool(size)
}

// GetBuffer returns a bytes from the pool with the given size.
func GetBuffer(size int) []byte {
	return defaultBuffers.GetBuffer(size)
}

// PutBuffer frees the bytes to the pool.
func PutBuffer(buf []byte) {
	defaultBuffers.PutBuffer(buf)
}
