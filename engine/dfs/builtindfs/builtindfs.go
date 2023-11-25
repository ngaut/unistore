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

package builtindfs

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ngaut/unistore/engine/dfs"
	"github.com/ngaut/unistore/engine/table/sstable"
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/scheduler"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type regionStores struct {
	stores []uint64
	expire time.Time
}

type storeFileAddr struct {
	meta     *metapb.Store
	fileAddr string
	expire   time.Time
}

type deferRemove struct {
	fileID uint64
	expire time.Time
}

var _ dfs.DFS = &BuiltinDFS{}

type BuiltinDFS struct {
	dir          string
	pd           pd.Client
	regionStores sync.Map
	fileAddrMap  sync.Map
	storeAddr    string
	scheduler    *scheduler.Scheduler

	deferRemoveMu  sync.Mutex
	deferRemoves   []deferRemove
	lastRemoveTime time.Time
}

func NewBuiltinDFS(pd pd.Client, dir string, storeAddr string) *BuiltinDFS {
	fs := &BuiltinDFS{
		dir:          dir,
		pd:           pd,
		regionStores: sync.Map{},
		fileAddrMap:  sync.Map{},
		storeAddr:    storeAddr,
		scheduler:    scheduler.NewScheduler(64),
	}
	http.Handle("/sst/", fs)
	return fs
}

func (fs *BuiltinDFS) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodPost:
		fs.handleUpload(writer, request)
	case http.MethodGet:
		fs.handleDownload(writer, request)
	default:
		http.Error(writer, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}
}

func (fs *BuiltinDFS) parseFileID(request *http.Request) (uint64, error) {
	var fileID uint64
	_, err := fmt.Sscanf(request.URL.Path, "/sst/%016x.sst", &fileID)
	if err != nil || fileID == 0 {
		return 0, errors.New("invalid file name")
	}
	return fileID, nil
}

func (fs *BuiltinDFS) handleDownload(writer http.ResponseWriter, request *http.Request) {
	fileID, err := fs.parseFileID(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	http.ServeFile(writer, request, sstable.NewFilename(fileID, fs.dir))
}

func (fs *BuiltinDFS) handleUpload(writer http.ResponseWriter, request *http.Request) {
	fileID, err := fs.parseFileID(request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	data := make([]byte, request.ContentLength)
	_, err = io.ReadFull(request.Body, data)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	err = fs.writeToLocal(data, sstable.NewFilename(fileID, fs.dir))
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (fs *BuiltinDFS) Open(fileID uint64, opts dfs.Options) (dfs.File, error) {
	size, err := fs.prefetch(fileID, opts)
	if err != nil {
		return nil, err
	}
	filename := sstable.NewFilename(fileID, fs.dir)
	fd, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return &File{id: fileID, size: size, fd: fd, fs: fs}, nil
}

func (fs *BuiltinDFS) Prefetch(fileID uint64, opts dfs.Options) error {
	_, err := fs.prefetch(fileID, opts)
	return err
}

func (fs *BuiltinDFS) prefetch(fileID uint64, opts dfs.Options) (size int64, err error) {
	localFileName := sstable.NewFilename(fileID, fs.dir)
	fi, err := os.Stat(localFileName)
	if err == nil {
		return fi.Size(), nil
	}
	data, err := fs.remoteReadFile(fileID, opts.ShardID)
	if err != nil {
		return 0, err
	}
	err = fs.writeToLocal(data, localFileName)
	if err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

func (fs *BuiltinDFS) writeToLocal(data []byte, localFileName string) error {
	tmpFileName := localFileName + ".tmp"
	fd, err := y.OpenTruncFile(tmpFileName, false)
	if err != nil {
		return err
	}
	_, err = fd.Write(data)
	if err != nil {
		fd.Close()
		return err
	}
	fd.Close()
	return os.Rename(tmpFileName, localFileName)
}

func (fs *BuiltinDFS) ReadFile(fileID uint64, opts dfs.Options) ([]byte, error) {
	localFileName := sstable.NewFilename(fileID, fs.dir)
	data, err := ioutil.ReadFile(localFileName)
	if err == nil {
		return data, nil
	}
	return fs.remoteReadFile(fileID, opts.ShardID)
}

func (fs *BuiltinDFS) Create(fileID uint64, data []byte, opts dfs.Options) error {
	fileAddrs, err := fs.getRemoteFileAddresses(opts.ShardID)
	if err != nil {
		return err
	}
	successCnt := 0
	quorumCnt := len(fileAddrs)
	if fs.dir != "" {
		quorumCnt += 1
		err = fs.writeToLocal(data, sstable.NewFilename(fileID, fs.dir))
		if err != nil {
			log.S().Errorf("local create file failed %v", err)
		} else {
			successCnt += 1
		}
	}
	for _, fileAddr := range fileAddrs {
		err = fs.remoteCreateFile(fileAddr, fileID, data)
		if err != nil {
			log.S().Errorf("remote %s create file failed %v", fileAddr, err)
			continue
		}
		successCnt += 1
	}
	if successCnt > quorumCnt/2 {
		return nil
	}
	return errors.Errorf("failed to create file")
}

func (fs *BuiltinDFS) Remove(fileID uint64, opts dfs.Options) error {
	var toBeRemove []deferRemove
	fs.deferRemoveMu.Lock()
	now := time.Now()
	if now.Sub(fs.lastRemoveTime) > time.Second {
		fs.lastRemoveTime = now
		for _, rm := range fs.deferRemoves {
			if rm.expire.Before(now) {
				toBeRemove = append(toBeRemove, rm)
			} else {
				break
			}
		}
		fs.deferRemoves = fs.deferRemoves[len(toBeRemove):]
	}
	fs.deferRemoves = append(fs.deferRemoves, deferRemove{fileID: fileID, expire: now.Add(removeExpireDuration)})
	fs.deferRemoveMu.Unlock()
	for _, rm := range toBeRemove {
		os.Remove(sstable.NewFilename(rm.fileID, fs.dir))
	}
	return nil
}

func (fs *BuiltinDFS) GetScheduler() *scheduler.Scheduler {
	return fs.scheduler
}

func (fs *BuiltinDFS) remoteReadFile(fileID, regionID uint64) ([]byte, error) {
	addrs, err := fs.getRemoteFileAddresses(regionID)
	if err != nil {
		return nil, err
	}
	rand.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})
	var data []byte
	for _, addr := range addrs {
		data, err = fs.remoteReadFileByAddr(addr, fileID)
		if err == nil {
			break
		} else {
			log.S().Errorf("region:%d failed to get file %d from %s", regionID, fileID, addr)
		}
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

var (
	regionExpireDuration = time.Second * 10
	storeExpireDuration  = time.Minute * 10
	removeExpireDuration = time.Minute * 10
)

func (fs *BuiltinDFS) getRemoteFileAddresses(regionID uint64) ([]string, error) {
	var rs *regionStores
	if val, ok := fs.regionStores.Load(regionID); ok {
		x := val.(*regionStores)
		if x.expire.After(time.Now()) {
			rs = x
		}
	}
	if rs == nil {
		region, err := fs.pd.GetRegionByID(context.Background(), regionID)
		if err != nil {
			log.S().Errorf("pd failed to get region by id %d, err %v", regionID, err)
			return nil, err
		}
		var storeIDs []uint64
		for _, peer := range region.Meta.GetPeers() {
			storeIDs = append(storeIDs, peer.StoreId)
		}
		rs = &regionStores{
			stores: storeIDs,
			expire: time.Now().Add(regionExpireDuration),
		}
		fs.regionStores.Store(regionID, rs)
	}
	var fileAddrs []string
	for _, storeID := range rs.stores {
		var addr *storeFileAddr
		if val, ok := fs.fileAddrMap.Load(storeID); ok {
			x := val.(*storeFileAddr)
			if x.expire.After(time.Now()) {
				addr = x
			}
		}
		if addr == nil {
			meta, err := fs.pd.GetStore(context.Background(), storeID)
			if err != nil {
				log.S().Errorf("pd failed to get store %d, err %v", storeID, err)
				return nil, err
			}
			addr = &storeFileAddr{
				meta:     meta,
				fileAddr: getFileAddress(meta),
				expire:   time.Now().Add(storeExpireDuration),
			}
			fs.fileAddrMap.Store(storeID, addr)
		}
		if addr.meta.Address != fs.storeAddr && addr.meta.State == metapb.StoreState_Up {
			fileAddrs = append(fileAddrs, addr.fileAddr)
		}
	}
	return fileAddrs, nil
}

func getFileAddress(meta *metapb.Store) string {
	idx := strings.LastIndexByte(meta.Address, ':')
	storeHost := meta.Address[:idx]
	idx = strings.LastIndexByte(meta.StatusAddress, ':')
	statusPort := meta.StatusAddress[idx:]
	return storeHost + statusPort
}

func (fs *BuiltinDFS) newFileURL(addr string, fileID uint64) string {
	return fmt.Sprintf("http://%s/sst/%016x.sst", addr, fileID)
}

func (fs *BuiltinDFS) remoteReadFileByAddr(addr string, fileID uint64) ([]byte, error) {
	resp, err := http.DefaultClient.Get(fs.newFileURL(addr, fileID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err = fs.checkRespError(resp); err != nil {
		return nil, err
	}
	buf := make([]byte, resp.ContentLength)
	_, err = io.ReadFull(resp.Body, buf)
	if err != nil {
		return nil, err
	}
	log.S().Infof("remote read %s file %d, size %d", addr, fileID, len(buf))
	return buf, nil
}

func (fs *BuiltinDFS) checkRespError(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		errMsg, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New(string(errMsg))
	}
	return nil
}

func (fs *BuiltinDFS) remoteCreateFile(addr string, fileID uint64, data []byte) error {
	req, err := http.NewRequest(http.MethodPost, fs.newFileURL(addr, fileID), nil)
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(data))
	req.Body = io.NopCloser(bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	return fs.checkRespError(resp)
}

var _ dfs.File = &File{}

type File struct {
	id   uint64
	size int64
	fd   *os.File
	fs   *BuiltinDFS
}

func (f *File) ID() uint64 {
	return f.id
}

func (f *File) Size() int64 {
	return f.size
}

func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	return f.fd.ReadAt(b, off)
}

func (f *File) InMem() bool {
	return false
}

func (f *File) MemSlice(off, length int) []byte {
	panic("not supported")
}

func (f *File) Close() error {
	f.fd.Close()
	// The file cache of BuiltinDFS is also the storage, we need to remove the file locally.
	return f.fs.Remove(f.id, dfs.Options{})
}
