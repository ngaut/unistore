package raftstore

import (
	"github.com/coocood/badger"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/zhangjinpeng1987/raft"
)

type RegionChangeEvent int

const (
	RegionChangeEvent_Create RegionChangeEvent = 0 + iota
	RegionChangeEvent_Update
	RegionChangeEvent_Destroy
)

type observerContext struct {
	region *metapb.Region
	bypass bool
}

type splitChecker interface {
	onKv(_ *observerContext, _ splitCheckKeyEntry) bool
	splitKeys() [][]byte
	approximateSplitKeys(_ *metapb.Region, _ *badger.DB) ([][]byte, error)
	policy() pdpb.CheckPolicy
}

type splitCheckerHost struct {
	autoSplit bool
	checkers  []*splitChecker
}

func (spCheckerHost *splitCheckerHost) skip() bool {
	//Todo, currently it is a place holder
	return false
}

func (spCheckerHost *splitCheckerHost) onKv() bool {
	// Todo: currently it is a place holder
	return false
}

func (spCheckerHost *splitCheckerHost) splitKeys() [][]byte {
	// Todo: currently it is a place holder
	return nil
}

func (spCheckerHost *splitCheckerHost) approximateSplitKeys(_ *metapb.Region, _ *badger.DB) ([][]byte, error) {
	// Todo: currently it is a place holder
	return nil, nil
}

func (spCheckerHost *splitCheckerHost) policy() pdpb.CheckPolicy {
	// Todo, currently it is a place hoder
	return 0
}

type registry struct {
}

type CoprocessorHost struct {
	// Todo: currently it is a place holder
	registry registry
}

func (c *CoprocessorHost) PrePropose(region *metapb.Region, req *raft_cmdpb.RaftCmdRequest) error {
	// Todo: currently it is a place holder
	return nil
}

func (c *CoprocessorHost) OnRegionChanged(region *metapb.Region, event RegionChangeEvent, role raft.StateType) {
	// Todo: currently it is a place holder
}

func (c *CoprocessorHost) OnRoleChanged(region *metapb.Region, role raft.StateType) {
	// Todo: currently it is a place holder
}

func (c *CoprocessorHost) newSplitCheckerHost(region *metapb.Region, engine *badger.DB, autoSplit bool,
	policy pdpb.CheckPolicy) *splitCheckerHost {
	//Todo, currently it is a place holder
	return nil
}

func (c *CoprocessorHost) shutdown() {}