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

package s3dfs

import (
	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/engine/dfs"
	"github.com/ngaut/unistore/engine/table/sstable"
	"github.com/ngaut/unistore/scheduler"
	"os"
)

var _ dfs.DFS = &S3DFS{}

type S3DFS struct {
	s3c      *S3Client
	localDir string
}

func NewS3DFS(localDir string, instanceID uint32, s3opts config.S3Options) (dfs.DFS, error) {
	s3c, err := NewS3Client(instanceID, s3opts)
	if err != nil {
		return nil, err
	}
	return &S3DFS{
		s3c:      s3c,
		localDir: localDir,
	}, nil
}

func (s *S3DFS) Open(fileID uint64, opts dfs.Options) (dfs.File, error) {
	err := s.Prefetch(fileID, opts)
	if err != nil {
		return nil, err
	}
	fd, err := os.Open(sstable.NewFilename(fileID, s.localDir))
	if err != nil {
		return nil, err
	}
	return newS3File(fileID, fd, s.s3c)
}

func (s *S3DFS) Prefetch(fileID uint64, opts dfs.Options) error {
	localFileName := sstable.NewFilename(fileID, s.localDir)
	_, err := os.Stat(localFileName)
	if err == nil {
		return nil
	}
	blockKey := s.s3c.BlockKey(fileID)
	tmpBlockFileName := localFileName + ".tmp"
	err = s.s3c.GetToFile(blockKey, tmpBlockFileName)
	if err != nil {
		return err
	}
	return os.Rename(tmpBlockFileName, localFileName)
}

func (s *S3DFS) ReadFile(fileID uint64, opts dfs.Options) ([]byte, error) {
	localFileName := sstable.NewFilename(fileID, s.localDir)
	data, err := os.ReadFile(localFileName)
	if err == nil {
		return data, nil
	}
	blockKey := s.s3c.BlockKey(fileID)
	return s.s3c.Get(blockKey, 0, 0)
}

func (s *S3DFS) Create(fileID uint64, data []byte, opts dfs.Options) error {
	blockKey := s.s3c.BlockKey(fileID)
	return s.s3c.Put(blockKey, data)
}

func (s *S3DFS) Remove(fileID uint64, opts dfs.Options) error {
	_ = os.Remove(sstable.NewFilename(fileID, s.localDir))
	s.s3c.SetExpired(fileID)
	return nil
}

func (s *S3DFS) GetScheduler() *scheduler.Scheduler {
	return s.s3c.Scheduler
}

var _ dfs.File = &s3File{}

type s3File struct {
	id   uint64
	s3c  *S3Client
	fd   *os.File
	size int64
}

func newS3File(id uint64, fd *os.File, s3c *S3Client) (dfs.File, error) {
	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	return &s3File{
		id:   id,
		fd:   fd,
		size: fi.Size(),
		s3c:  s3c,
	}, nil
}

func (s *s3File) ID() uint64 {
	return s.id
}

func (s *s3File) Size() int64 {
	return s.size
}

func (s *s3File) ReadAt(b []byte, off int64) (n int, err error) {
	return s.fd.ReadAt(b, off)
}

func (s *s3File) InMem() bool {
	return false
}

func (s *s3File) MemSlice(off, length int) []byte {
	panic("should not be called")
}

func (s *s3File) Close() error {
	name := s.fd.Name()
	_ = s.fd.Close()
	return os.Remove(name)
}
