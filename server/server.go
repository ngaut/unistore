package server

import (
	"context"
	"github.com/ngaut/unistore/sdb"
	"net/http"
	"os"
	"path/filepath"

	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/tikv"
	"github.com/ngaut/unistore/tikv/raftstore"
	"github.com/pingcap/badger"
	"github.com/pingcap/errors"
)

const (
	subPathRaft = "raft"
	subPathKV   = "kv"
)

func New(conf *config.Config, pdClient pd.Client) (*tikv.Server, error) {
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

	listener := raftstore.NewMetaChangeListener()
	allocator := &idAllocator{
		pdCli: pdClient,
	}
	raftDB, err := createRaftDB(subPathRaft, &conf.Engine)
	if err != nil {
		return nil, errors.AddStack(err)
	}
	recoverHandler, err := raftstore.NewRecoverHandler(raftDB)
	if err != nil {
		return nil, errors.AddStack(err)
	}
	safePoint := &tikv.SafePoint{}
	db, err := createKVDB(subPathKV, safePoint, listener, allocator, recoverHandler, &conf.Engine)
	if err != nil {
		return nil, errors.AddStack(err)
	}
	http.DefaultServeMux.HandleFunc("/debug/db", db.DebugHandler())
	engines := raftstore.NewEngines(db, raftDB, kvPath, raftPath, listener)
	innerServer := raftstore.NewRaftInnerServer(conf, engines, raftConf)
	innerServer.Setup(pdClient)
	router := innerServer.GetRaftstoreRouter()
	storeMeta := innerServer.GetStoreMeta()
	store := tikv.NewMVCCStore(conf, db, dbPath, safePoint, raftstore.NewDBWriter(conf, router), pdClient)
	rm := tikv.NewRaftRegionManager(storeMeta, router, store.DeadlockDetectSvr)
	innerServer.SetPeerEventObserver(rm)

	if err := innerServer.Start(pdClient); err != nil {
		return nil, errors.AddStack(err)
	}

	store.StartDeadlockDetection(true)
	return tikv.NewServer(rm, store, innerServer), nil
}

type idAllocator struct {
	pdCli pd.Client
}

func (a *idAllocator) AllocID() uint64 {
	physical, logical, tsErr := a.pdCli.GetTS(context.Background())
	if tsErr != nil {
		panic(tsErr)
	}
	return uint64(physical)<<18 + uint64(logical)
}

func setupRaftStoreConf(raftConf *raftstore.Config, conf *config.Config) {
	raftConf.Addr = conf.Server.StoreAddr

	// raftstore block
	raftConf.PdHeartbeatTickInterval = config.ParseDuration(conf.RaftStore.PdHeartbeatTickInterval)
	raftConf.RaftStoreMaxLeaderLease = config.ParseDuration(conf.RaftStore.RaftStoreMaxLeaderLease)
	raftConf.RaftBaseTickInterval = config.ParseDuration(conf.RaftStore.RaftBaseTickInterval)
	raftConf.RaftHeartbeatTicks = conf.RaftStore.RaftHeartbeatTicks
	raftConf.RaftElectionTimeoutTicks = conf.RaftStore.RaftElectionTimeoutTicks

	raftConf.SplitCheck.RegionMaxSize = uint64(conf.Server.RegionSize)
}

func createRaftDB(subPath string, conf *config.Engine) (*badger.DB, error) {
	opts := badger.DefaultOptions
	opts.NumCompactors = conf.NumCompactors
	opts.ValueThreshold = conf.ValueThreshold
	// Do not need to write blob for raft engine because it will be deleted soon.
	opts.ValueThreshold = 0
	opts.CompactionFilterFactory = raftstore.CreateRaftLogCompactionFilter
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
	opts.MaxIndexCacheSize = conf.BlockCacheSize / 4
	opts.TableBuilderOptions.SuRFStartLevel = conf.SurfStartLevel
	opts.CompactL0WhenClose = conf.CompactL0WhenClose
	opts.VolatileMode = conf.VolatileMode
	return badger.Open(opts)
}

func createKVDB(subPath string, safePoint *tikv.SafePoint, listener *raftstore.MetaChangeListener,
	allocator sdb.IDAllocator, recoverHandler *raftstore.RecoverHandler, conf *config.Engine) (*sdb.DB, error) {
	opts := sdb.DefaultOpt
	opts.MaxMemTableSize = conf.MaxMemTableSize
	opts.MaxBlockCacheSize = conf.BlockCacheSize
	opts.NumCompactors = conf.NumCompactors
	opts.CFs = []sdb.CFConfig{{Managed: true}, {Managed: false, ReadCommitted: true}, {Managed: true}}
	opts.S3Options.InstanceID = conf.S3.InstanceID
	opts.S3Options.EndPoint = conf.S3.Endpoint
	opts.S3Options.SecretKey = conf.S3.SecretKey
	opts.S3Options.KeyID = conf.S3.KeyID
	opts.S3Options.Bucket = conf.S3.Bucket
	opts.S3Options.Region = conf.S3.Region
	opts.Dir = filepath.Join(conf.DBPath, subPath)
	if safePoint != nil {
		opts.CompactionFilterFactory = safePoint.CreateCompactionFilter
	}
	if allocator != nil {
		opts.IDAllocator = allocator
	}
	opts.MetaChangeListener = listener
	opts.RecoverHandler = recoverHandler
	return sdb.OpenDB(opts)
}
