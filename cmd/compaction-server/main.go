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
	"github.com/ngaut/unistore/engine/compaction"
	"github.com/pingcap/log"
	"net/http"
	"runtime"
)

var (
	addr = flag.String("addr", ":9080", "serve address")
)

func main() {
	flag.Parse()
	runtime.SetMutexProfileFraction(10)
	server := compaction.NewServer()
	err := http.ListenAndServe(*addr, server)
	if err != nil {
		log.S().Error(err)
	}
}
