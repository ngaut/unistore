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

package compaction

import (
	"encoding/json"
	"github.com/ngaut/unistore/engine/table/sstable"
	"github.com/ngaut/unistore/enginepb"
	"github.com/ngaut/unistore/s3util"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"io"
	"net/http"
)

type Request struct {
	CF    int      `json:"cf"`
	Level int      `json:"level"`
	Tops  []uint64 `json:"tops"`

	// used for L1+ compaction
	Bottoms []uint64 `json:"bottoms"`

	// used for L0 compaction.
	MultiCFBottoms [][]uint64     `json:"multi_cf_bottoms"`
	Overlap        bool           `json:"overlap"`
	SafeTS         uint64         `json:"safe_ts"`
	BlockSize      int            `json:"block_size"`
	MaxTableSize   int64          `json:"max_table_size"`
	BloomFPR       float64        `json:"bloom_fpr"`
	InstanceID     uint32         `json:"instance_id"`
	S3             s3util.Options `json:"s_3"`
	FirstID        uint64         `json:"first_id"`
	LastID         uint64         `json:"last_id"`
}

func (req *Request) getTableBuilderOptions() sstable.TableBuilderOptions {
	return sstable.TableBuilderOptions{
		LogicalBloomFPR: req.BloomFPR,
		BlockSize:       req.BlockSize,
		MaxTableSize:    req.MaxTableSize,
	}
}

type Response struct {
	Error      string
	Compaction *enginepb.Compaction `json:"compaction"`
}

var _ http.Handler = &Server{}

type Server struct {
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	data := make([]byte, request.ContentLength)
	_, err := io.ReadFull(request.Body, data)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	req := new(Request)
	err = json.Unmarshal(data, req)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	c := y.NewCloser(1)
	defer c.Signal()
	s3c := s3util.NewS3Client(c, "", req.InstanceID, req.S3)
	resp := &Response{
		Compaction: &enginepb.Compaction{Cf: int32(req.CF), Level: uint32(req.Level), TopDeletes: req.Tops},
	}
	if req.Level == 0 {
		allResults, err1 := compactL0(req, s3c)
		if err1 != nil {
			log.Error("failed to compact L0", zap.Error(err1))
			resp.Error = err1.Error()
		} else {
			for cf := 0; cf < len(allResults); cf++ {
				cfResults := allResults[cf]
				for _, res := range cfResults {
					resp.Compaction.TableCreates = append(resp.Compaction.TableCreates, resultToTableCreate(cf, 1, res))
				}
			}
			for _, cfBots := range req.MultiCFBottoms {
				resp.Compaction.BottomDeletes = append(resp.Compaction.BottomDeletes, cfBots...)
			}
		}
	} else {
		stats := new(DiscardStats)
		results, err1 := CompactTables(req, stats, s3c)
		if err1 != nil {
			log.Error("failed to compact", zap.Int("L", req.Level), zap.Error(err1))
			resp.Error = err1.Error()
		} else {
			for _, res := range results {
				resp.Compaction.TableCreates = append(resp.Compaction.TableCreates, resultToTableCreate(req.CF, req.Level+1, res))
			}
			resp.Compaction.BottomDeletes = append(resp.Compaction.BottomDeletes, req.Bottoms...)
		}
	}
	respData, _ := json.Marshal(resp)
	_, err = writer.Write(respData)
	if err != nil {
		log.S().Error("failed to response to client %v", err)
	}
}

func resultToTableCreate(cf, level int, result *sstable.BuildResult) *enginepb.TableCreate {
	return &enginepb.TableCreate{
		CF:       int32(cf),
		Level:    uint32(level),
		ID:       result.ID,
		Smallest: result.Smallest,
		Biggest:  result.Biggest,
	}
}
