//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

package rocksdb

import (
	"math"

	"github.com/pierrec/lz4"
	"github.com/pingcap/errors"
)

var ErrDecompress = errors.New("Error during decompress")

func lz4Compress(input []byte) []byte {
	rawLen := len(input)
	if rawLen > math.MaxUint32 {
		return nil
	}

	var varintBuf [5]byte
	decompressedSize := encodeVarint32(varintBuf[:], uint32(rawLen))
	outputBound := lz4.CompressBlockBound(rawLen)
	output := make([]byte, len(decompressedSize)+outputBound)
	copy(output, decompressedSize)
	var ht [1 << 16]int
	n, err := lz4.CompressBlock(input, output[len(decompressedSize):], ht[:])
	if err != nil || n == 0 {
		return nil
	}
	return output[:len(decompressedSize)+n]
}

func isGoodCompressionRatio(compressed, raw []byte) bool {
	cl, rl := len(compressed), len(raw)
	return cl < rl-(rl/8)
}

func CompressBlock(raw []byte, tp CompressionType) ([]byte, bool) {
	var compressed []byte
	switch tp {
	case CompressionLz4:
		compressed = lz4Compress(raw)
	case CompressionNone:
		return raw, false
	case CompressionSnappy:
		panic("unsupported")
	case CompressionZstd:
		panic("unsupported")
	}
	if compressed == nil || !isGoodCompressionRatio(compressed, raw) {
		return raw, false
	}
	return compressed, true
}

func lz4Decompress(dst, raw []byte) ([]byte, error) {
	size, n := decodeVarint32(raw)
	if n <= 0 {
		return raw, ErrDecompress
	}

	if uint32(cap(dst)) < size {
		dst = make([]byte, size)
	} else {
		dst = dst[:size]
	}

	_, err := lz4.UncompressBlock(raw[n:], dst)
	return dst, err
}

func DecompressBlock(dst, raw []byte, tp CompressionType) ([]byte, error) {
	switch tp {
	case CompressionLz4:
		return lz4Decompress(dst, raw)
	case CompressionNone:
		return raw, nil
	case CompressionSnappy:
		panic("unsupported")
	case CompressionZstd:
		panic("unsupported")
	default:
		panic("unreachable branch")
	}
}
