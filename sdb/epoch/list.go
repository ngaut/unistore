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
	"sync/atomic"
	"unsafe"
)

type guardList struct {
	head unsafe.Pointer
}

func (l *guardList) add(g *Guard) {
	for {
		head := atomic.LoadPointer(&l.head)
		g.next = head
		if atomic.CompareAndSwapPointer(&l.head, head, unsafe.Pointer(g)) {
			return
		}
	}
}

func (l *guardList) iterate(f func(*Guard) bool) {
	loc := &l.head
	curr := (*Guard)(atomic.LoadPointer(&l.head))

	for curr != nil {
		delete := f(curr)

		next := curr.next
		// if current node is the head of list when start iteration
		// we cannot delete it from list, because the `it.list.head` may
		// point to a new node, if `it.loc` is updated we will lost the newly added nodes.
		if delete && loc != &l.head {
			*loc = next
		} else {
			loc = &curr.next
		}
		curr = (*Guard)(next)
	}
}
