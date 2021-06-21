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
	"bytes"
	"encoding/json"
	"github.com/ngaut/unistore/enginepb"
	"github.com/ngaut/unistore/s3util"
	"io"
	"net/http"
)

type Client struct {
	s3c       *s3util.S3Client
	remoteURL string
}

func (c *Client) Compact(req *Request) *Response {
	if c.remoteURL == "" {
		compResp := &enginepb.Compaction{TopDeletes: req.Tops, Level: uint32(req.Level), Cf: int32(req.CF)}
		resp := &Response{Compaction: compResp}
		if req.Level == 0 {
			results, err := compactL0(req, c.s3c)
			if err != nil {
				return &Response{Error: err.Error()}
			}
			for cf := 0; cf < len(req.MultiCFBottoms); cf++ {
				for _, res := range results[cf] {
					compResp.TableCreates = append(compResp.TableCreates, resultToTableCreate(cf, 1, res))
				}
				compResp.BottomDeletes = append(compResp.BottomDeletes, req.MultiCFBottoms[cf]...)
			}
		} else {
			discardStats := new(DiscardStats)
			results, err := CompactTables(req, discardStats, c.s3c)
			if err != nil {
				return &Response{Error: err.Error()}
			}
			for _, res := range results {
				compResp.TableCreates = append(compResp.TableCreates, resultToTableCreate(req.CF, req.Level+1, res))
			}
			compResp.BottomDeletes = append(compResp.BottomDeletes, req.Bottoms...)
		}
		return resp
	}
	reqData, err := json.Marshal(req)
	if err != nil {
		return &Response{Error: err.Error()}
	}
	httpResp, err := http.Post(c.remoteURL, "application/json", bytes.NewBuffer(reqData))
	if err != nil {
		return &Response{Error: err.Error()}
	}
	if httpResp.StatusCode != http.StatusOK {
		return &Response{Error: httpResp.Status}
	}
	data, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return &Response{Error: err.Error()}
	}
	compResp := new(Response)
	err = json.Unmarshal(data, compResp)
	if err != nil {
		return &Response{Error: err.Error()}
	}
	return compResp
}

func NewClient(addr string, s3c *s3util.S3Client) *Client {
	return &Client{remoteURL: addr, s3c: s3c}
}
