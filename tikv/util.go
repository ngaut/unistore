package tikv

import (
	"bytes"
	"sort"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

func exceedEndKey(current, endKey []byte) bool {
	return bytes.Compare(current, endKey) >= 0
}

func extractPhysicalTime(ts uint64) time.Time {
	t := int64(ts >> 18) // 18 is for the logical time.
	return time.Unix(t/1e3, (t%1e3)*1e6)
}

func tsSub(tsA, tsB uint64) time.Duration {
	tsAPhysical := int64(tsA >> 18)
	tsBPhysical := int64(tsB >> 18)
	return time.Duration(tsAPhysical-tsBPhysical) * time.Millisecond
}

// SortAndDedupHashVals will change hashVals into sort ascending order and remove duplicates
func sortAndDedupHashVals(hashVals []uint64) []uint64 {
	if len(hashVals) > 1 {
		sort.Slice(hashVals, func(i, j int) bool {
			return hashVals[i] < hashVals[j]
		})
		idx := 0
		for i, v := range hashVals {
			if i > 0 && hashVals[i] == hashVals[i-1] {
				continue
			}
			hashVals[idx] = v
			idx++
		}
		hashVals = hashVals[0:idx]
	}
	return hashVals
}

func mutationsToHashVals(mutations []*kvrpcpb.Mutation) []uint64 {
	hashVals := make([]uint64, len(mutations))
	for i, mut := range mutations {
		hashVals[i] = farm.Fingerprint64(mut.Key)
	}
	hashVals = sortAndDedupHashVals(hashVals)
	return hashVals
}

func keysToHashVals(keys ...[]byte) []uint64 {
	hashVals := make([]uint64, len(keys))
	for i, key := range keys {
		hashVals[i] = farm.Fingerprint64(key)
	}
	hashVals = sortAndDedupHashVals(hashVals)
	return hashVals
}

func safeCopy(b []byte) []byte {
	return append([]byte{}, b...)
}
