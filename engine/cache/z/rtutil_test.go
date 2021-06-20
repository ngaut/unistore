package z

import (
	"crypto/rand"
	"hash/fnv"
	"testing"

	"github.com/dgryski/go-farm"
)

func BenchmarkMemHash(b *testing.B) {
	buf := make([]byte, 64)
	rand.Read(buf)
	for i := 0; i < b.N; i++ {
		MemHash(buf)
	}
}

func BenchmarkSip(b *testing.B) {
	buf := make([]byte, 64)
	rand.Read(buf)
	for i := 0; i < b.N; i++ {
		SipHash(buf)
	}
}

func BenchmarkFarm(b *testing.B) {
	buf := make([]byte, 64)
	rand.Read(buf)
	for i := 0; i < b.N; i++ {
		farm.Fingerprint64(buf)
	}
}

func BenchmarkFnv(b *testing.B) {
	buf := make([]byte, 64)
	rand.Read(buf)
	f := fnv.New64a()
	for i := 0; i < b.N; i++ {
		f.Write(buf)
		f.Sum64()
		f.Reset()
	}
}

func SipHash(p []byte) (l, h uint64) {
	// Initialization.
	v0 := uint64(8317987320269560794) // k0 ^ 0x736f6d6570736575
	v1 := uint64(7237128889637516672) // k1 ^ 0x646f72616e646f6d
	v2 := uint64(7816392314733513934) // k0 ^ 0x6c7967656e657261
	v3 := uint64(8387220255325274014) // k1 ^ 0x7465646279746573
	t := uint64(len(p)) << 56

	// Compression.
	for len(p) >= 8 {

		m := uint64(p[0]) | uint64(p[1])<<8 | uint64(p[2])<<16 | uint64(p[3])<<24 |
			uint64(p[4])<<32 | uint64(p[5])<<40 | uint64(p[6])<<48 | uint64(p[7])<<56

		v3 ^= m

		// Round 1.
		v0 += v1
		v1 = v1<<13 | v1>>51
		v1 ^= v0
		v0 = v0<<32 | v0>>32

		v2 += v3
		v3 = v3<<16 | v3>>48
		v3 ^= v2

		v0 += v3
		v3 = v3<<21 | v3>>43
		v3 ^= v0

		v2 += v1
		v1 = v1<<17 | v1>>47
		v1 ^= v2
		v2 = v2<<32 | v2>>32

		// Round 2.
		v0 += v1
		v1 = v1<<13 | v1>>51
		v1 ^= v0
		v0 = v0<<32 | v0>>32

		v2 += v3
		v3 = v3<<16 | v3>>48
		v3 ^= v2

		v0 += v3
		v3 = v3<<21 | v3>>43
		v3 ^= v0

		v2 += v1
		v1 = v1<<17 | v1>>47
		v1 ^= v2
		v2 = v2<<32 | v2>>32

		v0 ^= m
		p = p[8:]
	}

	// Compress last block.
	switch len(p) {
	case 7:
		t |= uint64(p[6]) << 48
		fallthrough
	case 6:
		t |= uint64(p[5]) << 40
		fallthrough
	case 5:
		t |= uint64(p[4]) << 32
		fallthrough
	case 4:
		t |= uint64(p[3]) << 24
		fallthrough
	case 3:
		t |= uint64(p[2]) << 16
		fallthrough
	case 2:
		t |= uint64(p[1]) << 8
		fallthrough
	case 1:
		t |= uint64(p[0])
	}

	v3 ^= t

	// Round 1.
	v0 += v1
	v1 = v1<<13 | v1>>51
	v1 ^= v0
	v0 = v0<<32 | v0>>32

	v2 += v3
	v3 = v3<<16 | v3>>48
	v3 ^= v2

	v0 += v3
	v3 = v3<<21 | v3>>43
	v3 ^= v0

	v2 += v1
	v1 = v1<<17 | v1>>47
	v1 ^= v2
	v2 = v2<<32 | v2>>32

	// Round 2.
	v0 += v1
	v1 = v1<<13 | v1>>51
	v1 ^= v0
	v0 = v0<<32 | v0>>32

	v2 += v3
	v3 = v3<<16 | v3>>48
	v3 ^= v2

	v0 += v3
	v3 = v3<<21 | v3>>43
	v3 ^= v0

	v2 += v1
	v1 = v1<<17 | v1>>47
	v1 ^= v2
	v2 = v2<<32 | v2>>32

	v0 ^= t

	// Finalization.
	v2 ^= 0xff

	// Round 1.
	v0 += v1
	v1 = v1<<13 | v1>>51
	v1 ^= v0
	v0 = v0<<32 | v0>>32

	v2 += v3
	v3 = v3<<16 | v3>>48
	v3 ^= v2

	v0 += v3
	v3 = v3<<21 | v3>>43
	v3 ^= v0

	v2 += v1
	v1 = v1<<17 | v1>>47
	v1 ^= v2
	v2 = v2<<32 | v2>>32

	// Round 2.
	v0 += v1
	v1 = v1<<13 | v1>>51
	v1 ^= v0
	v0 = v0<<32 | v0>>32

	v2 += v3
	v3 = v3<<16 | v3>>48
	v3 ^= v2

	v0 += v3
	v3 = v3<<21 | v3>>43
	v3 ^= v0

	v2 += v1
	v1 = v1<<17 | v1>>47
	v1 ^= v2
	v2 = v2<<32 | v2>>32

	// Round 3.
	v0 += v1
	v1 = v1<<13 | v1>>51
	v1 ^= v0
	v0 = v0<<32 | v0>>32

	v2 += v3
	v3 = v3<<16 | v3>>48
	v3 ^= v2

	v0 += v3
	v3 = v3<<21 | v3>>43
	v3 ^= v0

	v2 += v1
	v1 = v1<<17 | v1>>47
	v1 ^= v2
	v2 = v2<<32 | v2>>32

	// Round 4.
	v0 += v1
	v1 = v1<<13 | v1>>51
	v1 ^= v0
	v0 = v0<<32 | v0>>32

	v2 += v3
	v3 = v3<<16 | v3>>48
	v3 ^= v2

	v0 += v3
	v3 = v3<<21 | v3>>43
	v3 ^= v0

	v2 += v1
	v1 = v1<<17 | v1>>47
	v1 ^= v2
	v2 = v2<<32 | v2>>32

	// return v0 ^ v1 ^ v2 ^ v3

	hash := v0 ^ v1 ^ v2 ^ v3
	h = hash >> 1
	l = hash << 1 >> 1
	return l, h

}
func BenchmarkNanoTime(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NanoTime()
	}
}

func BenchmarkCPUTicks(b *testing.B) {
	for i := 0; i < b.N; i++ {
		CPUTicks()
	}
}

func BenchmarkFastRand(b *testing.B) {
	for i := 0; i < b.N; i++ {
		FastRand()
	}
}
