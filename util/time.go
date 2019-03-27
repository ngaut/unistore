package util

import (
	"golang.org/x/sys/unix"
	"time"
)

type InstantType int8

const (
	Monotonic InstantType = iota
	MonotonicCoarse
)

/// todo, Instant has some methods.
type Instant struct {
	it InstantType
	/// todo, check needed, in TiKV, it uses a time library here.
	timespec unix.Timespec
}

/// todo, SlowTimer has some methods.
type SlowTimer struct {
	slowTime time.Duration
	t        Instant
}
