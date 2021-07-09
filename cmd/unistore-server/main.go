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
	"flag"
	"fmt"
	"github.com/ngaut/unistore/tikv"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/arl/statsviz"
	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/raft"
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
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
		conf.Engine.Path = *dataDir
	}
	if *logFile != "" {
		conf.Server.LogfilePath = *logFile
	}
}

type raftLogger struct {
	*zap.SugaredLogger
}

func (l raftLogger) Warning(v ...interface{}) {
	l.Warn(v...)
}
func (l raftLogger) Warningf(format string, v ...interface{}) {
	l.Warnf(format, v...)
}

func main() {
	flag.Parse()
	conf := loadConfig()
	loadCmdConf(conf)
	runtime.GOMAXPROCS(conf.Server.MaxProcs)
	runtime.SetMutexProfileFraction(10)
	debug.SetGCPercent(conf.Server.GCPercent)
	logger, p, err := log.InitLogger(&log.Config{
		Level: conf.Server.LogLevel,
		File: log.FileLogConfig{
			Filename: conf.Server.LogfilePath,
		},
	})
	if err != nil {
		panic(err)
	}
	log.ReplaceGlobals(logger, p)
	raft.SetLogger(raftLogger{logger.Sugar()})
	log.S().Infof("gitHash: %s", gitHash)
	log.S().Infof("conf %v", conf)

	go func() {
		log.S().Infof("listening on %v", conf.Server.StatusAddr)
		http.HandleFunc("/status", func(writer http.ResponseWriter, request *http.Request) {
			writer.WriteHeader(http.StatusOK)
		})
		statsviz.Register(http.DefaultServeMux)
		err := http.ListenAndServe(conf.Server.StatusAddr, nil)
		if err != nil {
			log.S().Fatal(err)
		}
	}()

	pdClient, err := pd.NewClient(strings.Split(conf.Server.PDAddr, ","), "")
	if err != nil {
		log.S().Fatal(err)
	}

	tikvServer, err := tikv.NewServer(conf, pdClient)
	if err != nil {
		log.Fatal("new server failed", zap.Error(err))
	}

	var alivePolicy = keepalive.EnforcementPolicy{
		MinTime:             2 * time.Second, // If a client pings more than once every 2 seconds, terminate the connection
		PermitWithoutStream: true,            // Allow pings even when there are no active streams
	}

	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(alivePolicy),
		grpc.InitialWindowSize(grpcInitialWindowSize),
		grpc.InitialConnWindowSize(grpcInitialConnWindowSize),
		grpc.MaxRecvMsgSize(20*1024*1024),
	)
	tikvpb.RegisterTikvServer(grpcServer, tikvServer)
	hsrv := health.NewServer()
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, hsrv)
	listenAddr := conf.Server.StoreAddr[strings.IndexByte(conf.Server.StoreAddr, ':'):]
	l, err := net.Listen("tcp", listenAddr)
	deadlock.RegisterDeadlockServer(grpcServer, tikvServer)
	if err != nil {
		log.S().Fatal(err)
	}
	handleSignal(grpcServer)
	err = grpcServer.Serve(l)
	if err != nil {
		log.S().Fatal(err)
	}
	tikvServer.Stop()
	log.Info("Server stopped.")
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
	return &conf
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
		log.S().Infof("Got signal [%s] to exit.", sig)
		grpcServer.Stop()
	}()
}
