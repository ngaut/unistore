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

package raftstore

import (
	"context"
	"encoding/binary"
	"github.com/ngaut/unistore/raft"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"net"
	"sync"
	"time"

	"github.com/ngaut/unistore/pd"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type raftConn struct {
	msgCh           chan *raft_serverpb.RaftMessage
	ctx             context.Context
	cancel          context.CancelFunc
	nextRetryTime   time.Time
	lastResolveTime time.Time
	addr            string
	storeID         uint64
	cfg             *Config

	pdCli        pd.Client
	batch        *tikvpb.BatchRaftMessage
	stream       tikvpb.Tikv_BatchRaftClient
	streamCancel context.CancelFunc

	rawConn net.Conn
	rawBuf  []byte
}

func newRaftConn(storeID uint64, cfg *Config, pdCli pd.Client) *raftConn {
	ctx, cancel := context.WithCancel(context.Background())
	rc := &raftConn{
		msgCh:   make(chan *raft_serverpb.RaftMessage, 4096),
		ctx:     ctx,
		cancel:  cancel,
		storeID: storeID,
		cfg:     cfg,
		pdCli:   pdCli,
		batch:   new(tikvpb.BatchRaftMessage),
	}
	go rc.runSender()
	return rc
}

const (
	maxBatchCount = 128
	maxBatchSize  = 10 * 1024 * 1024
)

func (c *raftConn) runSender() {
	for {
		select {
		case msg := <-c.msgCh:
			c.sendRawMsg(msg)
		case <-c.ctx.Done():
			log.Info("raftConn done")
			return
		}
	}
}

func (c *raftConn) senderHandleMsg(msg *raft_serverpb.RaftMessage) {
	c.resetBatchRaftMsg()
	batch := c.batch
	totalSize := c.getEstimatedSize(msg)
	batch.Msgs = append(batch.Msgs, msg)
	chLen := len(c.msgCh)
	for i := 0; i < chLen && i < maxBatchCount && totalSize < maxBatchSize; i++ {
		newMsg := <-c.msgCh
		totalSize += c.getEstimatedSize(newMsg)
		batch.Msgs = append(batch.Msgs, newMsg)
	}
	var err error
	if c.stream == nil {
		if time.Now().Before(c.nextRetryTime) {
			// drop the messages directly.
			return
		}
		err = c.newStream()
		if err != nil {
			c.nextRetryTime = time.Now().Add(time.Second)
			log.Warn("failed to create raft stream", zap.Error(err))
			return
		}
		log.Info("new raft stream")
	}
	err = c.stream.Send(batch)
	if err != nil {
		c.streamCancel()
		c.stream = nil
		log.Warn("failed to send batch raft message", zap.Error(err))
	}
}

func (c *raftConn) sendRawMsg(msg *raft_serverpb.RaftMessage) {
	c.encodeRawMsg(msg)
	chLen := len(c.msgCh)
	for i := 0; i < chLen; i++ {
		newMsg := <-c.msgCh
		c.encodeRawMsg(newMsg)
	}
	var err error
	if c.rawConn == nil {
		if time.Now().Before(c.nextRetryTime) {
			// drop the messages directly.
			return
		}
		err = c.newRawConn()
		if err != nil {
			c.nextRetryTime = time.Now().Add(time.Second)
			log.Warn("failed to create raft raw conn", zap.Error(err))
			return
		}
		log.Info("new raft raw conn")
	}
	_, err = c.rawConn.Write(c.rawBuf)
	if err != nil {
		c.nextRetryTime = time.Now().Add(time.Second)
		log.Warn("failed to write raft raw message", zap.Error(err))
		c.rawConn.Close()
		c.rawConn = nil
	}
	c.rawBuf = c.rawBuf[:0]
}

var raftMsgPool = sync.Pool{New: func() interface{} {
	return &raft_serverpb.RaftMessage{}
}}

func (c *raftConn) encodeRawMsg(msg *raft_serverpb.RaftMessage) {
	size := msg.Size()
	data := c.allocFromRawBuf(size)
	y.Assert(len(data) == size)
	_, err := msg.MarshalTo(data)
	y.Assert(err == nil)
	pbMsg := msg.Message
	*pbMsg = eraftpb.Message{}
	raft.MessagePool.Put(pbMsg)
	*msg = raft_serverpb.RaftMessage{}
	raftMsgPool.Put(msg)
}

func (c *raftConn) allocFromRawBuf(size int) []byte {
	oldLen := len(c.rawBuf)
	newLen := oldLen + 4 + size
	if newLen <= cap(c.rawBuf) {
		c.rawBuf = c.rawBuf[:newLen]
	} else {
		oldBuf := c.rawBuf
		c.rawBuf = make([]byte, newLen, newLen*5/4)
		copy(c.rawBuf, oldBuf)
	}
	binary.LittleEndian.PutUint32(c.rawBuf[oldLen:], uint32(size))
	return c.rawBuf[oldLen+4:]
}

// getEstimatedSize only count the entry data size for better performance.
func (c *raftConn) getEstimatedSize(msg *raft_serverpb.RaftMessage) int {
	var size int
	for _, entry := range msg.GetMessage().GetEntries() {
		size += len(entry.Data)
	}
	return size
}

func (c *raftConn) resetBatchRaftMsg() {
	for i := 0; i < len(c.batch.Msgs); i++ {
		c.batch.Msgs[i] = nil
	}
	c.batch.Msgs = c.batch.Msgs[:0]
}

const resolveRefreshInterval = time.Second * 60

func (c *raftConn) resolveAddr() (string, error) {
	if c.addr != "" && time.Since(c.lastResolveTime) < resolveRefreshInterval {
		return c.addr, nil
	}
	addr, err := getStoreAddr(c.storeID, c.pdCli)
	if err != nil {
		return "", err
	}
	c.addr = addr
	c.lastResolveTime = time.Now()
	return c.addr, nil
}

func (c *raftConn) newStream() error {
	addr, err := c.resolveAddr()
	if err != nil {
		return err
	}
	cc, err := grpc.Dial(addr, grpc.WithInsecure(),
		grpc.WithInitialWindowSize(int32(c.cfg.GrpcInitialWindowSize)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                c.cfg.GrpcKeepAliveTime,
			Timeout:             c.cfg.GrpcKeepAliveTimeout,
			PermitWithoutStream: true,
		}))
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithCancel(c.ctx)
	c.stream, err = tikvpb.NewTikvClient(cc).BatchRaft(ctx)
	if err != nil {
		return err
	}
	c.streamCancel = cancelFunc
	return err
}

func (c *raftConn) newRawConn() error {
	addr, err := c.resolveAddr()
	if err != nil {
		return err
	}
	dialer := &net.Dialer{
		Timeout:   3 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return err
	}
	_, err = conn.Write([]byte("raft"))
	if err != nil {
		return err
	}
	c.rawConn = conn
	return nil
}

func (c *raftConn) Stop() {
	c.cancel()
}

func (c *raftConn) Send(msg *raft_serverpb.RaftMessage) error {
	select {
	case c.msgCh <- msg:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}

type connKey struct {
	storeID uint64
	index   int
}

type RaftClient struct {
	config *Config
	sync.RWMutex
	conns map[connKey]*raftConn
	pdCli pd.Client
}

func newRaftClient(config *Config, pdCli pd.Client) *RaftClient {
	return &RaftClient{
		config: config,
		conns:  make(map[connKey]*raftConn),
		pdCli:  pdCli,
	}
}

func (c *RaftClient) getConn(storeID, regionID uint64) *raftConn {
	index := hashRegionID(regionID) % c.config.GrpcRaftConnNum
	key := connKey{storeID, int(index)}
	c.Lock()
	defer c.Unlock()
	conn, ok := c.conns[key]
	if ok {
		return conn
	}
	conn = newRaftConn(storeID, c.config, c.pdCli)
	c.conns[key] = conn
	return conn
}

func (c *RaftClient) Send(msg *raft_serverpb.RaftMessage) {
	storeID := msg.GetToPeer().GetStoreId()
	conn := c.getConn(storeID, msg.GetRegionId())
	conn.Send(msg)
}

func (c *RaftClient) Flush() {
	// Not support BufferHint
}

func (c *RaftClient) Stop() {
	c.Lock()
	defer c.Unlock()
	for k, conn := range c.conns {
		delete(c.conns, k)
		conn.Stop()
	}
}

func getStoreAddr(id uint64, pdCli pd.Client) (string, error) {
	store, err := pdCli.GetStore(context.TODO(), id)
	if err != nil {
		return "", err
	}
	if store.GetState() == metapb.StoreState_Tombstone {
		return "", errors.Errorf("store %d has been removed", id)
	}
	addr := store.GetAddress()
	if addr == "" {
		return "", errors.Errorf("invalid empty address for store %d", id)
	}
	return addr, nil
}
