// Copyright 2021-present PingCAP, Inc.
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

package regiontree

import (
	"bytes"
	"github.com/google/btree"
	"github.com/pingcap/kvproto/pkg/metapb"
)

// btreeItem is BTree's Item that uses []byte to compare.
type btreeItem struct {
	key    []byte
	inf    bool
	region *metapb.Region
}

func newBtreeItem(region *metapb.Region) *btreeItem {
	return &btreeItem{
		key:    region.EndKey,
		inf:    len(region.EndKey) == 0,
		region: region,
	}
}

func newBtreeSearchItem(key []byte) *btreeItem {
	return &btreeItem{
		key: key,
	}
}

func (item *btreeItem) Less(o btree.Item) bool {
	other := o.(*btreeItem)
	if item.inf {
		return false
	}
	if other.inf {
		return true
	}
	return bytes.Compare(item.key, other.key) < 0
}

type RegionTree struct {
	tree *btree.BTree
}

func NewRegionTree() *RegionTree {
	return &RegionTree{
		tree: btree.New(32),
	}
}

func (r *RegionTree) GetRegionByKey(key []byte) (region *metapb.Region) {
	r.tree.AscendGreaterOrEqual(newBtreeSearchItem(key), func(item btree.Item) bool {
		region = item.(*btreeItem).region
		if bytes.Equal(region.EndKey, key) {
			region = nil
			return true
		}
		return false
	})
	return
}

func (r *RegionTree) Put(region *metapb.Region) (notExist bool) {
	old := r.tree.ReplaceOrInsert(newBtreeItem(region))
	return old == nil
}

func (r *RegionTree) Delete(region *metapb.Region) bool {
	old := r.tree.Delete(newBtreeItem(region))
	return old != nil
}

func (r *RegionTree) Iterate(start, end []byte, fn func(region *metapb.Region) bool) {
	r.tree.AscendGreaterOrEqual(newBtreeSearchItem(start), func(item btree.Item) bool {
		reg := item.(*btreeItem).region
		if bytes.Equal(reg.EndKey, start) {
			return true
		}
		if len(end) > 0 && bytes.Compare(reg.StartKey, end) >= 0 {
			return false
		}
		return fn(reg)
	})
}
