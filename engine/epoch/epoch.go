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

import "sync/atomic"

// The least significant bit of epoch is active flag.
type epoch uint64

func (e epoch) isActive() bool {
	return uint64(e)&1 == 1
}

func (e epoch) activate() epoch {
	return epoch(uint64(e) | 1)
}

func (e epoch) deactivate() epoch {
	return epoch(uint64(e) & ^uint64(1))
}

func (e epoch) sub(a epoch) int {
	return int((uint64(e) >> 1) - (uint64(a) >> 1))
}

func (e epoch) successor() epoch {
	return epoch(uint64(e) + 2)
}

type atomicEpoch struct {
	epoch uint64
}

func (e *atomicEpoch) load() epoch {
	return (epoch)(atomic.LoadUint64(&e.epoch))
}

func (e *atomicEpoch) store(new epoch) {
	atomic.StoreUint64(&e.epoch, uint64(new))
}
