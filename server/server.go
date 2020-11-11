package server

import (
	"github.com/ngaut/unistore/tikv/raftstore"
	"os"
	"path/filepath"

	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/tikv"
	"github.com/pingcap/badger"
)

const (
	subPathRaft = "raft"
	subPathKV   = "kv"
)

func NewMock(conf *config.Config, clusterID uint64) (*tikv.Server, *tikv.MockRegionManager, *tikv.MockPD, error) {
	safePoint := &tikv.SafePoint{}
	db, err := createShardingDB(subPathKV, safePoint, &conf.Engine)
	if err != nil {
		return nil, nil, nil, err
	}

	rm, err := tikv.NewMockRegionManager(db, clusterID, tikv.RegionOptions{
		StoreAddr:  conf.Server.StoreAddr,
		PDAddr:     conf.Server.PDAddr,
		RegionSize: conf.Server.RegionSize,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	pdClient := tikv.NewMockPD(rm)
	svr, err := setupStandAlongInnerServer(db, safePoint, rm, pdClient, conf)
	if err != nil {
		return nil, nil, nil, err
	}
	return svr, rm, pdClient, nil
}

func New(conf *config.Config, pdClient pd.Client) (*tikv.Server, error) {
	safePoint := &tikv.SafePoint{}
	db, err := createShardingDB(subPathKV, safePoint, &conf.Engine)
	if err != nil {
		return nil, err
	}
	if conf.Server.Raft {
		return setupRaftServer(db, safePoint, pdClient, conf)
	}

	rm := tikv.NewStandAloneRegionManager(db, getRegionOptions(conf), pdClient)
	return setupStandAlongInnerServer(db, safePoint, rm, pdClient, conf)
}

func getRegionOptions(conf *config.Config) tikv.RegionOptions {
	return tikv.RegionOptions{
		StoreAddr:  conf.Server.StoreAddr,
		PDAddr:     conf.Server.PDAddr,
		RegionSize: conf.Server.RegionSize,
	}
}

func setupRaftServer(db *badger.ShardingDB, safePoint *tikv.SafePoint, pdClient pd.Client, conf *config.Config) (*tikv.Server, error) {
	dbPath := conf.Engine.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	snapPath := filepath.Join(dbPath, "snap")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)
	os.Mkdir(snapPath, os.ModePerm)

	raftConf := raftstore.NewDefaultConfig()
	raftConf.SnapPath = snapPath
	setupRaftStoreConf(raftConf, conf)

	raftDB, err := createDB(subPathRaft, nil, &conf.Engine)
	if err != nil {
		return nil, err
	}

	engines := raftstore.NewEngines(db, raftDB, kvPath, raftPath)

	innerServer := raftstore.NewRaftInnerServer(conf, engines, raftConf)
	innerServer.Setup(pdClient)
	router := innerServer.GetRaftstoreRouter()
	storeMeta := innerServer.GetStoreMeta()
	store := tikv.NewMVCCStore(conf, db, dbPath, safePoint, raftstore.NewDBWriter(conf, router), pdClient)
	rm := tikv.NewRaftRegionManager(storeMeta, router, store.DeadlockDetectSvr)
	innerServer.SetPeerEventObserver(rm)

	if err := innerServer.Start(pdClient); err != nil {
		return nil, err
	}

	store.StartDeadlockDetection(true)

	return tikv.NewServer(rm, store, innerServer), nil
}

func setupStandAlongInnerServer(db *badger.ShardingDB, safePoint *tikv.SafePoint, rm tikv.RegionManager, pdClient pd.Client, conf *config.Config) (*tikv.Server, error) {
	innerServer := tikv.NewStandAlongInnerServer(db)
	innerServer.Setup(pdClient)
	store := tikv.NewMVCCStore(conf, db, conf.Engine.DBPath, safePoint, tikv.NewDBWriter(db), pdClient)
	store.DeadlockDetectSvr.ChangeRole(tikv.Leader)

	if err := innerServer.Start(pdClient); err != nil {
		return nil, err
	}

	store.StartDeadlockDetection(false)

	return tikv.NewServer(rm, store, innerServer), nil
}

func setupRaftStoreConf(raftConf *raftstore.Config, conf *config.Config) {
	raftConf.Addr = conf.Server.StoreAddr

	// raftstore block
	raftConf.PdHeartbeatTickInterval = config.ParseDuration(conf.RaftStore.PdHeartbeatTickInterval)
	raftConf.RaftStoreMaxLeaderLease = config.ParseDuration(conf.RaftStore.RaftStoreMaxLeaderLease)
	raftConf.RaftBaseTickInterval = config.ParseDuration(conf.RaftStore.RaftBaseTickInterval)
	raftConf.RaftHeartbeatTicks = conf.RaftStore.RaftHeartbeatTicks
	raftConf.RaftElectionTimeoutTicks = conf.RaftStore.RaftElectionTimeoutTicks

	// coprocessor block
	raftConf.SplitCheck.RegionMaxKeys = uint64(conf.Coprocessor.RegionMaxKeys)
	raftConf.SplitCheck.RegionSplitKeys = uint64(conf.Coprocessor.RegionSplitKeys)
}

func createDB(subPath string, safePoint *tikv.SafePoint, conf *config.Engine) (*badger.DB, error) {
	opts := badger.DefaultOptions
	opts.NumCompactors = conf.NumCompactors
	opts.ValueThreshold = conf.ValueThreshold
	if subPath == subPathRaft {
		// Do not need to write blob for raft engine because it will be deleted soon.
		opts.ValueThreshold = 0
		opts.CompactionFilterFactory = raftstore.CreateRaftLogCompactionFilter
	} else {
		opts.ManagedTxns = true
		opts.S3Options.InstanceID = conf.S3.InstanceID
		opts.S3Options.EndPoint = conf.S3.Endpoint
		opts.S3Options.SecretKey = conf.S3.SecretKey
		opts.S3Options.KeyID = conf.S3.KeyID
		opts.S3Options.Bucket = conf.S3.Bucket
	}
	opts.ValueLogWriteOptions.WriteBufferSize = 4 * 1024 * 1024
	opts.Dir = filepath.Join(conf.DBPath, subPath)
	opts.ValueDir = opts.Dir
	opts.ValueLogFileSize = conf.VlogFileSize
	opts.ValueLogMaxNumFiles = 3
	opts.MaxMemTableSize = conf.MaxMemTableSize
	opts.TableBuilderOptions.MaxTableSize = conf.MaxTableSize
	opts.NumMemtables = conf.NumMemTables
	opts.NumLevelZeroTables = conf.NumL0Tables
	opts.NumLevelZeroTablesStall = conf.NumL0TablesStall
	opts.LevelOneSize = conf.L1Size
	opts.SyncWrites = conf.SyncWrite
	opts.MaxBlockCacheSize = conf.BlockCacheSize
	opts.MaxIndexCacheSize = conf.IndexCacheSize
	opts.TableBuilderOptions.SuRFStartLevel = conf.SurfStartLevel
	if safePoint != nil {
		opts.CompactionFilterFactory = safePoint.CreateCompactionFilter
	}
	opts.CompactL0WhenClose = conf.CompactL0WhenClose
	opts.VolatileMode = conf.VolatileMode
	return badger.Open(opts)
}

func createShardingDB(subPath string, safePoint *tikv.SafePoint, conf *config.Engine) (*badger.ShardingDB, error) {
	opts := badger.ShardingDBDefaultOpt
	opts.NumCompactors = conf.NumCompactors
	opts.CFs = []badger.CFConfig{{Managed: true}, {Managed: false}, {Managed: true}, {Managed: false}}
	opts.S3Options.InstanceID = conf.S3.InstanceID
	opts.S3Options.EndPoint = conf.S3.Endpoint
	opts.S3Options.SecretKey = conf.S3.SecretKey
	opts.S3Options.KeyID = conf.S3.KeyID
	opts.S3Options.Bucket = conf.S3.Bucket
	opts.Dir = filepath.Join(conf.DBPath, subPath)
	opts.ValueDir = opts.Dir
	if safePoint != nil {
		opts.CompactionFilterFactory = safePoint.CreateCompactionFilter
	}
	return badger.OpenShardingDB(opts)
}
