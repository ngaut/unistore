package raftstore

import "github.com/pingcap/kvproto/pkg/metapb"

func CheckKeyInRegion(key []byte, region *metapb.Region) error {
	if key >= region.StartKey && (len(region.EndKey) == 0 || key < region.EndKey) {
		return nil
	} else {
		return &ErrKeyNotInRegion{ Key: key, Region: region }
	}
}
