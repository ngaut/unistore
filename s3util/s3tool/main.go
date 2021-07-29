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
	"fmt"
	"io"
	"os"

	"github.com/ngaut/unistore/s3util"
	"github.com/ngaut/unistore/scheduler"
	"path/filepath"
)

const (
	List    = "list"
	Clean   = "clean"
	Backup  = "backup"
	Recover = "recover"
)

var (
	EndPoint, Region, Bucket, KeyID, SecretKey string
	InstanceID                                 int
)

func init() {

	flag.StringVar(&EndPoint, "endpoint", "", "Endpoint.")
	flag.StringVar(&KeyID, "key-id", "", "Key ID.")
	flag.StringVar(&SecretKey, "secret-key", "", "Secret Key.")
	flag.StringVar(&Bucket, "bucket", "", "Bucket name.")
	flag.StringVar(&Region, "region", "", "Region name.")
	flag.IntVar(&InstanceID, "instance-id", 0, "Instance ID.")
	flag.Parse()
}

func main() {
	args := flag.Args()
	var dir string
	if len(args) < 1 || (args[0] != List && args[0] != Clean && args[0] != Backup && args[0] != Recover) {
		fmt.Println("s3tool [operate] [dir]")
		fmt.Println("$ s3tool list")
		fmt.Println("$ s3tool clean")
		fmt.Println("$ s3tool backup ./")
		fmt.Println("$ s3tool recover ./")
		return
	} else if args[0] == Backup || args[0] == Recover {
		if len(args) < 2 {
			fmt.Println("The dir is null")
			return
		}
		dir = args[1]
	}
	opts := s3util.Options{
		KeyID:     KeyID,
		SecretKey: SecretKey,
		EndPoint:  EndPoint,
		Bucket:    Bucket,
		Region:    Region,
	}
	c := s3util.NewS3Client(nil, dir, uint32(InstanceID), opts)
	if fileIDs, err := c.ListFiles(); err != nil {
		panic(err)
	} else {
		switch args[0] {
		case List:
			list(c, fileIDs)
		case Clean:
			cleanup(c, fileIDs)
		case Backup:
			backup(c, dir, fileIDs)
		case Recover:
			recover(c, dir)
		}
	}
}

func list(c *s3util.S3Client, fileIDs map[uint64]struct{}) {
	for fid := range fileIDs {
		key := c.BlockKey(fid)
		fmt.Println(key)
	}
}

func cleanup(c *s3util.S3Client, fileIDs map[uint64]struct{}) {
	bt := scheduler.NewBatchTasks()
	for fid := range fileIDs {
		key := c.BlockKey(fid)
		bt.AppendTask(func() error {
			return c.Delete(key)
		})
	}
	if err := c.BatchSchedule(bt); err != nil {
		panic(err)
	}
}

func backup(c *s3util.S3Client, dir string, fileIDs map[uint64]struct{}) {
	bt := scheduler.NewBatchTasks()
	for fid := range fileIDs {
		key := c.BlockKey(fid)
		bt.AppendTask(func() error {
			return c.GetToFile(key, filepath.Join(dir, key))
		})
	}
	if err := c.BatchSchedule(bt); err != nil {
		panic(err)
	}
}

func recover(c *s3util.S3Client, dir string) {
	bt := scheduler.NewBatchTasks()
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		key := info.Name()
		filePath := path
		if len(filePath) > 4 && filePath[len(filePath)-4:] == ".sst" {
			bt.AppendTask(func() error {
				f, err := os.Open(filePath)
				if err != nil {
					panic(err)
				}
				data, err := io.ReadAll(f)
				if err != nil {
					panic(err)
				}
				f.Close()
				return c.Put(key, data)
			})
		}
		return nil
	})
	if err := c.BatchSchedule(bt); err != nil {
		panic(err)
	}
}
