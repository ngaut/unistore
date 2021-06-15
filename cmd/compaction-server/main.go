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
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/sdb/compaction"
	"github.com/pingcap/log"
	"net/http"
	"runtime"
	"strings"
)

var (
	pdAddr = flag.String("pd", "127.0.0.1:2379", "pd address")
	addr   = flag.String("addr", ":9080", "serve address")
)

func main() {
	flag.Parse()
	runtime.SetMutexProfileFraction(10)
	pdClient, err := pd.NewClient(strings.Split(*pdAddr, ","), "compaction")
	if err != nil {
		log.S().Fatal(err)
	}
	server := compaction.NewServer(pdClient)
	err = http.ListenAndServe(*addr, server)
	if err != nil {
		log.S().Error(err)
	}
}
