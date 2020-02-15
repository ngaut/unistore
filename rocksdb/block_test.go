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

package rocksdb

import (
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockReadWrite(t *testing.T) {
	nums := sortedNumbers(1000)

	builder := newBlockBuilder(16)
	for _, num := range nums {
		builder.Add(encodeKey(num), []byte(num))
	}
	block := builder.Finish()

	var i int
	iter := newBlockIterator(block)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := decodeKey(iter.Key())
		value := string(iter.Value())
		expected := nums[i]

		require.Equal(t, expected, key)
		require.Equal(t, expected, value)
		i++
	}
}

func encodeKey(key string) []byte {
	var ikey InternalKey
	ikey.UserKey = []byte(key)
	return ikey.Encode()
}

func decodeKey(data []byte) string {
	return string(extractUserKey(data))
}

func sortedNumbers(size int) []string {
	nums := make([]string, size)
	for i := range nums {
		nums[i] = strconv.Itoa(i)
	}
	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})
	return nums
}
