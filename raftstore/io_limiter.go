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

package raftstore

import (
	"io"

	"golang.org/x/time/rate"
)

// IOLimiter controls how frequently events are allowed to happen.
type IOLimiter = rate.Limiter

// NewInfLimiter returns a new IOLimiter.
func NewInfLimiter() *IOLimiter {
	return rate.NewLimiter(rate.Inf, 0)
}

// LimitWriter represents a limit writer.
type LimitWriter struct {
	writer io.Writer
}

func (lw *LimitWriter) Write(b []byte) (int, error) {
	return lw.writer.Write(b)
}
