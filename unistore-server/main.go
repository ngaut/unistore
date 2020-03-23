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

package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coocood/badger"
	"github.com/coocood/badger/options"
	"github.com/coocood/badger/y"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/tikv"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/ngaut/unistore/tikv/raftstore"
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	configPath    = flag.String("config", "", "config file path")
	pdAddr        = flag.String("pd", "", "pd address")
	storeAddr     = flag.String("addr", "", "store address")
	advertiseAddr = flag.String("advertise-addr", "", "advertise address")
	statusAddr    = flag.String("status-addr", "", "status address")
	dataDir       = flag.String("data-dir", "", "data directory")
	logFile       = flag.String("log-file", "", "log file")
	configCheck   = flagBoolean("config-check", false, "check config file validity and exit")
)

var (
	gitHash = "None"
)

const (
	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30

	subPathRaft = "raft"
	subPathKV   = "kv"
)

func flagBoolean(name string, defaultVal bool, usage string) *bool {
	if !defaultVal {
		usage = fmt.Sprintf("%s (default false)", usage)
		return flag.Bool(name, defaultVal, usage)
	}
	return flag.Bool(name, defaultVal, usage)
}

// loadCmdConf will overwrite configurations using command line arguments
func loadCmdConf(conf *config.Config) {
	if *pdAddr != "" {
		conf.Server.PDAddr = *pdAddr
	}
	if *storeAddr != "" {
		conf.Server.StoreAddr = *storeAddr
	}
	if *advertiseAddr != "" {
		conf.Server.StoreAddr = *advertiseAddr
	}
	if *statusAddr != "" {
		conf.Server.StatusAddr = *statusAddr
	}
	if *dataDir != "" {
		conf.Engine.DBPath = *dataDir
	}
	if *logFile != "" {
		conf.Server.LogfilePath = *logFile
	}
}

func main() {
	flag.Parse()
	conf := loadConfig()
	loadCmdConf(conf)
	runtime.GOMAXPROCS(conf.Server.MaxProcs)
	runtime.SetMutexProfileFraction(10)
	if conf.Server.LogfilePath != "" {
		err := log.SetOutputByName(conf.Server.LogfilePath)
		if err != nil {
			panic(err)
		}
	}
	log.Info("gitHash:", gitHash)
	log.SetLevelByString(conf.Server.LogLevel)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Infof("conf %v", conf)
	config.SetGlobalConf(conf)
	db := createDB(subPathKV, &conf.Engine)
	bundle := &mvcc.DBBundle{
		DB:            db,
		LockStore:     lockstore.NewMemStore(8 << 20),
		RollbackStore: lockstore.NewMemStore(256 << 10),
	}

	pdClient, err := pd.NewClient(strings.Split(conf.Server.PDAddr, ","), "")
	if err != nil {
		log.Fatal(err)
	}

	var (
		innerServer   tikv.InnerServer
		store         *tikv.MVCCStore
		regionManager tikv.RegionManager
	)
	if conf.Server.Raft {
		innerServer, store, regionManager = setupRaftInnerServer(bundle, pdClient, conf)
	} else {
		innerServer, store, regionManager = setupStandAlongInnerServer(bundle, pdClient, conf)
	}
	err = store.StartDeadlockDetection(context.Background(), pdClient, innerServer, conf.Server.Raft)
	if err != nil {
		log.Fatal("StartDeadlockDetection error=%v", err)
	}

	tikvServer := tikv.NewServer(regionManager, store, innerServer)

	var alivePolicy = keepalive.EnforcementPolicy{
		MinTime:             2 * time.Second, // If a client pings more than once every 2 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(alivePolicy),
		grpc.InitialWindowSize(grpcInitialWindowSize),
		grpc.InitialConnWindowSize(grpcInitialConnWindowSize),
		grpc.MaxRecvMsgSize(10*1024*1024),
	)
	tikvpb.RegisterTikvServer(grpcServer, tikvServer)
	listenAddr := conf.Server.StoreAddr[strings.IndexByte(conf.Server.StoreAddr, ':'):]
	l, err := net.Listen("tcp", listenAddr)
	deadlock.RegisterDeadlockServer(grpcServer, tikvServer)
	if err != nil {
		log.Fatal(err)
	}
	handleSignal(grpcServer)
	go func() {
		log.Infof("listening on %v", conf.Server.StatusAddr)
		http.HandleFunc("/status", func(writer http.ResponseWriter, request *http.Request) {
			writer.WriteHeader(http.StatusOK)
		})
		err := http.ListenAndServe(conf.Server.StatusAddr, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()
	err = grpcServer.Serve(l)
	if err != nil {
		log.Fatal(err)
	}
	tikvServer.Stop()
	log.Info("Server stopped.")

	err = store.Close()
	if err != nil {
		log.Fatal(err)
	}
	log.Info("Store closed.")

	if err = regionManager.Close(); err != nil {
		log.Fatal(err)
	}

	err = innerServer.Stop()
	if err != nil {
		log.Fatal(err)
	}
}

func loadConfig() *config.Config {
	conf := config.DefaultConf
	if *configPath != "" {
		_, err := toml.DecodeFile(*configPath, &conf)
		if err != nil {
			if *configCheck {
				fmt.Fprintf(os.Stderr, "config check failed, err=%s\n", err.Error())
				os.Exit(1)
			}
			panic(err)
		}
		if *configCheck {
			os.Exit(0)
		}
	} else {
		// configCheck should have the config file specified.
		if *configCheck {
			fmt.Fprintln(os.Stderr, "config check failed, no config file specified for config-check")
			os.Exit(1)
		}
	}
	y.Assert(len(conf.Engine.Compression) >= badger.DefaultOptions.TableBuilderOptions.MaxLevels)
	return &conf
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

func setupRaftInnerServer(bundle *mvcc.DBBundle, pdClient pd.Client, conf *config.Config) (tikv.InnerServer, *tikv.MVCCStore, tikv.RegionManager) {
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

	raftDB := createDB(subPathRaft, &conf.Engine)
	meta, err := bundle.LockStore.LoadFromFile(filepath.Join(kvPath, raftstore.LockstoreFileName))
	if err != nil {
		log.Fatal(err)
	}
	var offset uint64
	if meta != nil {
		offset = binary.LittleEndian.Uint64(meta)
	}
	err = raftstore.RestoreLockStore(offset, bundle, raftDB)
	if err != nil {
		log.Fatal(err)
	}

	engines := raftstore.NewEngines(bundle, raftDB, kvPath, raftPath)

	innerServer := raftstore.NewRaftInnerServer(engines, raftConf)
	innerServer.Setup(pdClient)
	router := innerServer.GetRaftstoreRouter()
	storeMeta := innerServer.GetStoreMeta()
	store := tikv.NewMVCCStore(bundle, dbPath, raftstore.NewDBWriter(router), pdClient)
	rm := tikv.NewRaftRegionManager(storeMeta, router, store.DeadlockDetectSvr)
	innerServer.SetPeerEventObserver(rm)

	if err := innerServer.Start(pdClient); err != nil {
		log.Fatal(err)
	}

	return innerServer, store, rm
}

func setupStandAlongInnerServer(bundle *mvcc.DBBundle, pdClient pd.Client, conf *config.Config) (tikv.InnerServer, *tikv.MVCCStore, tikv.RegionManager) {
	regionOpts := tikv.RegionOptions{
		StoreAddr:  conf.Server.StoreAddr,
		PDAddr:     conf.Server.PDAddr,
		RegionSize: conf.Server.RegionSize,
	}

	innerServer := tikv.NewStandAlongInnerServer(bundle)
	innerServer.Setup(pdClient)
	store := tikv.NewMVCCStore(bundle, conf.Engine.DBPath, tikv.NewDBWriter(bundle), pdClient)
	store.DeadlockDetectSvr.ChangeRole(tikv.Leader)
	rm := tikv.NewStandAloneRegionManager(bundle.DB, regionOpts, pdClient)

	if err := innerServer.Start(pdClient); err != nil {
		log.Fatal(err)
	}

	return innerServer, store, rm
}

func createDB(subPath string, conf *config.Engine) *badger.DB {
	opts := badger.DefaultOptions
	opts.NumCompactors = conf.NumCompactors
	opts.ValueThreshold = conf.ValueThreshold
	if subPath == subPathRaft {
		// Do not need to write blob for raft engine because it will be deleted soon.
		opts.ValueThreshold = 0
	} else {
		opts.ManagedTxns = true
	}
	opts.ValueLogWriteOptions.WriteBufferSize = 4 * 1024 * 1024
	opts.Dir = filepath.Join(conf.DBPath, subPath)
	opts.ValueDir = opts.Dir
	opts.ValueLogFileSize = conf.VlogFileSize
	opts.MaxMemTableSize = conf.MaxMemTableSize
	opts.MaxTableSize = conf.MaxTableSize
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
	opts.MaxCacheSize = conf.BlockCacheSize
	opts.TableBuilderOptions.SuRFStartLevel = conf.SurfStartLevel
	opts.CompactionFilterFactory = tikv.CreateCompactionFilter
	opts.CompactL0WhenClose = conf.CompactL0WhenClose
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func handleSignal(grpcServer *grpc.Server) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sigCh
		log.Infof("Got signal [%s] to exit.", sig)
		grpcServer.Stop()
	}()
}
