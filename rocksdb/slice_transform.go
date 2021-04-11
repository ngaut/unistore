//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

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

var (
	_ SliceTransform = new(FixedPrefixSliceTransform)
	_ SliceTransform = new(FixedSuffixSliceTransform)
	_ SliceTransform = new(NoopSliceTransform)
)

// SliceTransform can be used as a extractor.
type SliceTransform interface {
	Transform([]byte) []byte
	InDomain([]byte) bool
	InRange([]byte) bool
}

// FixedPrefixSliceTransform represents the fixed prefix SliceTransform.
type FixedPrefixSliceTransform struct {
	prefixLen int
}

// NewFixedPrefixSliceTransform returns a new fixed prefix SliceTransform.
func NewFixedPrefixSliceTransform(prefixLen int) *FixedPrefixSliceTransform {
	return &FixedPrefixSliceTransform{prefixLen: prefixLen}
}

// Transform implements the SliceTransform Transform method.
func (st *FixedPrefixSliceTransform) Transform(key []byte) []byte {
	return key[:st.prefixLen]
}

// InDomain implements the SliceTransform InDomain method.
func (st *FixedPrefixSliceTransform) InDomain(key []byte) bool {
	return len(key) >= st.prefixLen
}

// InRange implements the SliceTransform InRange method.
func (st *FixedPrefixSliceTransform) InRange(key []byte) bool {
	return true
}

// FixedSuffixSliceTransform represents the fixed suffix SliceTransform.
type FixedSuffixSliceTransform struct {
	suffixLen int
}

// NewFixedSuffixSliceTransform returns a new fixed suffix SliceTransform.
func NewFixedSuffixSliceTransform(suffixLen int) *FixedSuffixSliceTransform {
	return &FixedSuffixSliceTransform{suffixLen: suffixLen}
}

// Transform implements the SliceTransform Transform method.
func (st *FixedSuffixSliceTransform) Transform(key []byte) []byte {
	mid := len(key) - st.suffixLen
	return key[:mid]
}

// InDomain implements the SliceTransform InDomain method.
func (st *FixedSuffixSliceTransform) InDomain(key []byte) bool {
	return len(key) >= st.suffixLen
}

// InRange implements the SliceTransform InRange method.
func (st *FixedSuffixSliceTransform) InRange(key []byte) bool {
	return true
}

// NoopSliceTransform represents the noop SliceTransform.
type NoopSliceTransform struct{}

// NewNoopSliceTransform returns a new noop SliceTransform.
func NewNoopSliceTransform() *NoopSliceTransform {
	return &NoopSliceTransform{}
}

// Transform implements the SliceTransform Transform method.
func (st *NoopSliceTransform) Transform(key []byte) []byte {
	return key
}

// InDomain implements the SliceTransform InDomain method.
func (st *NoopSliceTransform) InDomain(key []byte) bool {
	return true
}

// InRange implements the SliceTransform InRange method.
func (st *NoopSliceTransform) InRange(key []byte) bool {
	return true
}
