// Copyright 2021-present PingCAP, Inc.
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
	"github.com/BurntSushi/toml"
	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/engine/compaction"
	"github.com/ngaut/unistore/engine/dfs/s3dfs"
	"github.com/pingcap/log"
	"net/http"
	"runtime"
)

var (
	configPath = flag.String("config", "", "config file path")
)

type Config struct {
	Addr       string           `toml:"addr"`
	InstanceID uint32           `toml:"instance-id"`
	S3         config.S3Options `toml:"s3"`
}

func main() {
	flag.Parse()
	runtime.SetMutexProfileFraction(10)
	conf := &Config{
		Addr:       ":9080",
		InstanceID: 1,
	}
	_, err := toml.DecodeFile(*configPath, conf)
	if err != nil {
		panic(err)
	}
	s3fs, err := s3dfs.NewS3DFS("", conf.InstanceID, conf.S3)
	if err != nil {
		panic(err)
	}
	server, err := compaction.NewServer(conf.InstanceID, s3fs)
	if err != nil {
		panic(err)
	}
	err = http.ListenAndServe(conf.Addr, server)
	if err != nil {
		log.S().Error(err)
	}
}
