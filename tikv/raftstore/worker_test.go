package raftstore

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestStalePeerInfo(t *testing.T) {
	time := time.Now()
	peerInfo := &stalePeerInfo{
		regionId: 1,
		endKey: []byte{1, 2, 10},
		timeout: time,
	}
	data := peerInfo.serializeStalePeerInfo()
	another := deserializeStalePeerInfo(data)
	assert.Equal(t, another.regionId, peerInfo.regionId)
	assert.Equal(t, another.endKey, peerInfo.endKey)
	assert.Equal(t, another.regionId, peerInfo.regionId)
	assert.True(t, peerInfo.timeout.Equal(another.timeout))
}
