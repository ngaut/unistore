package sdb

import "errors"

var (
	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("Key not found")

	errShardNotFound            = errors.New("shard not found")
	errShardNotMatch            = errors.New("shard not match")
	errShardWrongSplittingState = errors.New("shard wrong splitting state")
)
