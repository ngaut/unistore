package server

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/ngaut/unistore/config"
	unistoretikv "github.com/ngaut/unistore/tikv"
	"github.com/ngaut/unistore/tikv/raftstore"
	"github.com/pingcap/badger"
	"github.com/pingcap/badger/options"
	tidbconfig "github.com/pingcap/tidb/store/mockstore/unistore/config"
	"github.com/pingcap/tidb/store/mockstore/unistore/lockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore/pd"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/mvcc"
)

const (
	subPathRaft = "raft"
	subPathKV   = "kv"
)

func New(conf *config.Config, pdClient pd.Client) (*tikv.Server, error) {
	physical, logical, err := pdClient.GetTS(context.Background())
	if err != nil {
		return nil, err
	}
	ts := uint64(physical)<<18 + uint64(logical)

	safePoint := &tikv.SafePoint{}
	db, err := createDB(subPathKV, safePoint, &conf.Engine)
	if err != nil {
		return nil, err
	}
	bundle := &mvcc.DBBundle{
		DB:        db,
		LockStore: lockstore.NewMemStore(8 << 20),
		StateTS:   ts,
	}
	if conf.Server.Raft {
		return setupRaftServer(bundle, safePoint, pdClient, conf)
	}

	rm := tikv.NewStandAloneRegionManager(bundle, getRegionOptions(conf), pdClient)
	return setupStandAlongInnerServer(bundle, safePoint, rm, pdClient, conf)
}

func getRegionOptions(conf *config.Config) tikv.RegionOptions {
	return tikv.RegionOptions{
		StoreAddr:  conf.Server.StoreAddr,
		PDAddr:     conf.Server.PDAddr,
		RegionSize: conf.Server.RegionSize,
	}
}

func setupRaftServer(bundle *mvcc.DBBundle, safePoint *tikv.SafePoint, pdClient pd.Client, conf *config.Config) (*tikv.Server, error) {
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
	meta, err := bundle.LockStore.LoadFromFile(filepath.Join(kvPath, raftstore.LockstoreFileName))
	if err != nil {
		return nil, err
	}
	var offset uint64
	if meta != nil {
		offset = binary.LittleEndian.Uint64(meta)
	}
	err = raftstore.RestoreLockStore(offset, bundle, raftDB)
	if err != nil {
		return nil, err
	}

	engines := raftstore.NewEngines(bundle, raftDB, kvPath, raftPath)

	innerServer := raftstore.NewRaftInnerServer(conf, engines, raftConf)
	innerServer.Setup(pdClient)
	router := innerServer.GetRaftstoreRouter()
	storeMeta := innerServer.GetStoreMeta()
	store := tikv.NewMVCCStore(convertToTidbConfig(conf), bundle, dbPath, safePoint, raftstore.NewDBWriter(conf, router), pdClient)
	rm := unistoretikv.NewRaftRegionManager(storeMeta, router, store.DeadlockDetectSvr)
	innerServer.SetPeerEventObserver(rm)

	if err := innerServer.Start(pdClient); err != nil {
		return nil, err
	}

	store.StartDeadlockDetection(true)

	return tikv.NewServer(rm, store, innerServer), nil
}

func setupStandAlongInnerServer(bundle *mvcc.DBBundle, safePoint *tikv.SafePoint, rm tikv.RegionManager, pdClient pd.Client, conf *config.Config) (*tikv.Server, error) {
	innerServer := tikv.NewStandAlongInnerServer(bundle)
	innerServer.Setup(pdClient)
	store := tikv.NewMVCCStore(convertToTidbConfig(conf), bundle, conf.Engine.DBPath, safePoint, tikv.NewDBWriter(bundle), pdClient)
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
	compressionPerLevel := make([]options.CompressionType, len(conf.Compression))
	for i := range opts.TableBuilderOptions.CompressionPerLevel {
		compressionPerLevel[i] = config.ParseCompression(conf.Compression[i])
	}
	opts.TableBuilderOptions.CompressionPerLevel = compressionPerLevel
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

func convertToTidbConfig(conf *config.Config) *tidbconfig.Config {
	return &tidbconfig.Config{
		Server: tidbconfig.Server{
			PDAddr:      conf.Server.PDAddr,
			StoreAddr:   conf.Server.StoreAddr,
			StatusAddr:  conf.Server.StatusAddr,
			LogLevel:    conf.Server.LogLevel,
			RegionSize:  conf.Server.RegionSize,
			MaxProcs:    conf.Server.MaxProcs,
			Raft:        conf.Server.Raft,
			LogfilePath: conf.Server.LogfilePath,
		},
		RaftStore: tidbconfig.RaftStore{
			PdHeartbeatTickInterval:  conf.RaftStore.PdHeartbeatTickInterval,
			RaftStoreMaxLeaderLease:  conf.RaftStore.RaftStoreMaxLeaderLease,
			RaftBaseTickInterval:     conf.RaftStore.RaftBaseTickInterval,
			RaftHeartbeatTicks:       conf.RaftStore.RaftHeartbeatTicks,
			RaftElectionTimeoutTicks: conf.RaftStore.RaftElectionTimeoutTicks,
			CustomRaftLog:            conf.RaftStore.CustomRaftLog,
		},
		Engine: tidbconfig.Engine{
			DBPath:             conf.Engine.DBPath,
			ValueThreshold:     conf.Engine.ValueThreshold,
			MaxMemTableSize:    conf.Engine.MaxMemTableSize,
			MaxTableSize:       conf.Engine.MaxTableSize,
			L1Size:             conf.Engine.L1Size,
			NumMemTables:       conf.Engine.NumMemTables,
			NumL0Tables:        conf.Engine.NumL0Tables,
			NumL0TablesStall:   conf.Engine.NumL0TablesStall,
			VlogFileSize:       conf.Engine.VlogFileSize,
			SyncWrite:          conf.Engine.SyncWrite,
			NumCompactors:      conf.Engine.NumCompactors,
			SurfStartLevel:     conf.Engine.SurfStartLevel,
			BlockCacheSize:     conf.Engine.BlockCacheSize,
			IndexCacheSize:     conf.Engine.IndexCacheSize,
			Compression:        conf.Engine.Compression,
			IngestCompression:  conf.Engine.IngestCompression,
			VolatileMode:       conf.Engine.VolatileMode,
			CompactL0WhenClose: conf.Engine.CompactL0WhenClose,
		},
		Coprocessor: tidbconfig.Coprocessor{
			RegionMaxKeys:   conf.Coprocessor.RegionMaxKeys,
			RegionSplitKeys: conf.Coprocessor.RegionSplitKeys,
		},
		PessimisticTxn: tidbconfig.PessimisticTxn{
			WaitForLockTimeout:  conf.PessimisticTxn.WaitForLockTimeout,
			WakeUpDelayDuration: conf.PessimisticTxn.WakeUpDelayDuration,
		},
	}
}
