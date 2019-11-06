// Copyright 2017 PingCAP, Inc.
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

package pd

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"google.golang.org/grpc"
)

// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	GetClusterID(ctx context.Context) uint64
	AllocID(ctx context.Context) (uint64, error)
	Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (*pdpb.BootstrapResponse, error)
	IsBootstrapped(ctx context.Context) (bool, error)
	PutStore(ctx context.Context, store *metapb.Store) error
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	GetAllStores(ctx context.Context, excludeTombstone bool) ([]*metapb.Store, error)
	GetClusterConfig(ctx context.Context) (*metapb.Cluster, error)
	GetRegion(ctx context.Context, key []byte) (*metapb.Region, error)
	GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, error)
	ReportRegion(*pdpb.RegionHeartbeatRequest)
	AskSplit(ctx context.Context, region *metapb.Region) (*pdpb.AskSplitResponse, error)
	AskBatchSplit(ctx context.Context, region *metapb.Region, count int) (*pdpb.AskBatchSplitResponse, error)
	ReportBatchSplit(ctx context.Context, regions []*metapb.Region) error
	GetGCSafePoint(ctx context.Context) (uint64, error)
	StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error
	SetRegionHeartbeatResponseHandler(h func(*pdpb.RegionHeartbeatResponse))
	Close()
}

const (
	pdTimeout             = time.Second
	reconnectInterval     = time.Second
	maxInitClusterRetries = 100
	maxRetryCount         = 10
)

var (
	// errFailInitClusterID is returned when failed to load clusterID from all supplied PD addresses.
	errFailInitClusterID = errors.New("[pd] failed to get cluster id")
)

type client struct {
	url       []string
	tag       string
	clusterID uint64

	connMu        sync.RWMutex
	clientConn    *grpc.ClientConn
	members       *pdpb.GetMembersResponse
	stream        pdpb.PD_RegionHeartbeatClient
	streamCtx     context.Context
	streamCancel  context.CancelFunc
	lastReconnect time.Time

	receiveRegionHeartbeatCh chan *pdpb.RegionHeartbeatResponse
	regionCh                 chan *pdpb.RegionHeartbeatRequest
	pendingRequest           *pdpb.RegionHeartbeatRequest

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	heartbeatHandler atomic.Value
}

// NewClient creates a PD client.
func NewClient(pdAddrs []string, tag string) (Client, error) {
	log.Infof("[%s][pd] create pd client with endpoints %v", tag, pdAddrs)
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		url:                      pdAddrs,
		receiveRegionHeartbeatCh: make(chan *pdpb.RegionHeartbeatResponse, 1),
		ctx:                      ctx,
		cancel:                   cancel,
		tag:                      tag,
		regionCh:                 make(chan *pdpb.RegionHeartbeatRequest, 64),
	}
	cc, members, err := connectToEndpoints(ctx, pdAddrs)
	if err != nil {
		return nil, err
	}
	c.clusterID = members.Header.GetClusterId()
	c.clientConn = cc
	c.members = members
	log.Infof("[%s][pd] init cluster id %v", tag, c.clusterID)
	c.wg.Add(1)
	if err := c.createHeartbeatStream(); err != nil {
		return nil, err
	}
	go c.heartbeatStreamLoop()

	return c, nil
}

func (c *client) reconnect() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if time.Since(c.lastReconnect) < reconnectInterval {
		// Avoid unnecessary updating.
		return nil
	}
	c.clientConn.Close()

	cc, members, err := tryConnectLeader(c.ctx, c.members)
	if err != nil {
		return err
	}
	c.clientConn = cc
	c.members = members

	if err := c.createHeartbeatStream(); err != nil {
		return err
	}
	c.lastReconnect = time.Now()
	return nil
}

func (c *client) doRequest(ctx context.Context, f func(context.Context, pdpb.PDClient) error) error {
	var err error
	var retry int
	for {
		c.connMu.RLock()
		ctx, cancel := context.WithTimeout(ctx, pdTimeout)
		err = f(ctx, pdpb.NewPDClient(c.clientConn))
		c.connMu.RUnlock()
		cancel()
		if err == nil {
			return nil
		}
		
		for {
			if err := c.reconnect(); err == nil {
				break
			}

			log.Warnf("reconnect failed, error: %s", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(reconnectInterval):
				if retry >= maxRetryCount {
					return errors.New("failed too many times")
				}
				retry++
			}
		}
	}
}

func (c *client) createHeartbeatStream() error {
	var (
		stream pdpb.PD_RegionHeartbeatClient
		err    error
		cancel context.CancelFunc
		ctx    context.Context
	)
	ctx, cancel = context.WithCancel(c.ctx)
	client := pdpb.NewPDClient(c.clientConn)
	stream, err = client.RegionHeartbeat(ctx)
	if err != nil {
		cancel()
		log.Errorf("[%s][pd] create region heartbeat stream error: %v", c.tag, err)
		return err
	}
	log.Error("recreate heartbeat stream")
	if c.stream != nil {
		// Try to cancel an unused heartbeat sender.
		c.streamCancel()
	}
	c.stream = stream
	c.streamCtx = ctx
	c.streamCancel = cancel
	return nil
}

func (c *client) heartbeatStreamLoop() {
	defer c.wg.Done()
	for {
		errCh := make(chan error, 1)
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go c.reportRegionHeartbeat(errCh, wg)
		go c.receiveRegionHeartbeat(errCh, wg)
		select {
		case err := <-errCh:
			log.Warnf("[%s][pd] heartbeat stream get error: %s ", c.tag, err)
			for {
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(reconnectInterval):
					if err := c.reconnect(); err == nil {
						break
					}
				}
			}
		case <-c.ctx.Done():
			log.Info("cancel heartbeat stream loop")
			return
		}
		wg.Wait()
	}
}

func (c *client) receiveRegionHeartbeat(errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		c.connMu.RLock()
		resp, err := c.stream.Recv()
		c.connMu.RUnlock()
		if err != nil {
			errCh <- err
			return
		}

		if h := c.heartbeatHandler.Load(); h != nil {
			h.(func(*pdpb.RegionHeartbeatResponse))(resp)
		}
	}
}

func (c *client) reportRegionHeartbeat(errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()

	if c.pendingRequest != nil {
		c.connMu.RLock()
		err := c.stream.Send(c.pendingRequest)
		c.connMu.RUnlock()
		if err != nil {
			errCh <- err
			return
		}
		c.pendingRequest = nil
	}

	for {
		select {
		case <-c.streamCtx.Done():
			return
		case request, ok := <-c.regionCh:
			if !ok {
				return
			}
			request.Header = c.requestHeader()
			c.connMu.RLock()
			err := c.stream.Send(request)
			c.connMu.RUnlock()
			if err != nil {
				c.pendingRequest = request
				errCh <- err
				return
			}
		}
	}
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()

	if err := c.clientConn.Close(); err != nil {
		log.Errorf("[%s][pd] failed close grpc clientConn: %v", c.tag, err)
	}
}

func (c *client) GetClusterID(context.Context) uint64 {
	return c.clusterID
}

func (c *client) AllocID(ctx context.Context) (uint64, error) {
	var resp *pdpb.AllocIDResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.AllocID(ctx, &pdpb.AllocIDRequest{
			Header: c.requestHeader(),
		})
		return err1
	})
	if err != nil {
		return 0, err
	}
	return resp.GetId(), nil
}

func (c *client) Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (resp *pdpb.BootstrapResponse, err error) {
	err = c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.Bootstrap(ctx, &pdpb.BootstrapRequest{
			Header: c.requestHeader(),
			Store:  store,
			Region: region,
		})
		return err1
	})
	return resp, err
}

func (c *client) IsBootstrapped(ctx context.Context) (bool, error) {
	var resp *pdpb.IsBootstrappedResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.IsBootstrapped(ctx, &pdpb.IsBootstrappedRequest{Header: c.requestHeader()})
		return err1
	})
	if err != nil {
		return false, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return false, errors.New(herr.String())
	}
	return resp.Bootstrapped, nil
}

func (c *client) PutStore(ctx context.Context, store *metapb.Store) error {
	var resp *pdpb.PutStoreResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.PutStore(ctx, &pdpb.PutStoreRequest{
			Header: c.requestHeader(),
			Store:  store,
		})
		return err1
	})
	if err != nil {
		return err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return errors.New(herr.String())
	}
	return nil
}

func (c *client) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	var resp *pdpb.GetStoreResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.GetStore(ctx, &pdpb.GetStoreRequest{
			Header:  c.requestHeader(),
			StoreId: storeID,
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Store, nil
}

func (c *client) GetAllStores(ctx context.Context, excludeTombstone bool) ([]*metapb.Store, error) {
	var resp *pdpb.GetAllStoresResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.GetAllStores(ctx, &pdpb.GetAllStoresRequest{
			Header:                 c.requestHeader(),
			ExcludeTombstoneStores: excludeTombstone,
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Stores, nil
}

func (c *client) GetClusterConfig(ctx context.Context) (*metapb.Cluster, error) {
	var resp *pdpb.GetClusterConfigResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.GetClusterConfig(ctx, &pdpb.GetClusterConfigRequest{
			Header: c.requestHeader(),
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Cluster, nil
}

func (c *client) GetRegion(ctx context.Context, key []byte) (*metapb.Region, error) {
	var resp *pdpb.GetRegionResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.GetRegion(ctx, &pdpb.GetRegionRequest{
			Header:    c.requestHeader(),
			RegionKey: key,
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Region, nil
}

func (c *client) GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, error) {
	var resp *pdpb.GetRegionResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.GetRegionByID(ctx, &pdpb.GetRegionByIDRequest{
			Header:   c.requestHeader(),
			RegionId: regionID,
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Region, nil
}

func (c *client) AskSplit(ctx context.Context, region *metapb.Region) (resp *pdpb.AskSplitResponse, err error) {
	err = c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.AskSplit(ctx, &pdpb.AskSplitRequest{
			Header: c.requestHeader(),
			Region: region,
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp, nil
}

func (c *client) AskBatchSplit(ctx context.Context, region *metapb.Region, count int) (resp *pdpb.AskBatchSplitResponse, err error) {
	err = c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.AskBatchSplit(ctx, &pdpb.AskBatchSplitRequest{
			Header:     c.requestHeader(),
			Region:     region,
			SplitCount: uint32(count),
		})
		return err1
	})
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp, nil
}

func (c *client) ReportBatchSplit(ctx context.Context, regions []*metapb.Region) error {
	var resp *pdpb.ReportBatchSplitResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.ReportBatchSplit(ctx, &pdpb.ReportBatchSplitRequest{
			Header:  c.requestHeader(),
			Regions: regions,
		})
		return err1
	})
	if err != nil {
		return err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return errors.New(herr.String())
	}
	return nil
}

func (c *client) GetGCSafePoint(ctx context.Context) (uint64, error) {
	var resp *pdpb.GetGCSafePointResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.GetGCSafePoint(ctx, &pdpb.GetGCSafePointRequest{
			Header: c.requestHeader(),
		})
		return err1
	})
	if err != nil {
		return 0, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return 0, errors.New(herr.String())
	}
	return resp.SafePoint, nil
}

func (c *client) StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error {
	var resp *pdpb.StoreHeartbeatResponse
	err := c.doRequest(ctx, func(ctx context.Context, client pdpb.PDClient) error {
		var err1 error
		resp, err1 = client.StoreHeartbeat(ctx, &pdpb.StoreHeartbeatRequest{
			Header: c.requestHeader(),
			Stats:  stats,
		})
		return err1
	})
	if err != nil {
		return err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return errors.New(herr.String())
	}
	return nil
}

func (c *client) ReportRegion(request *pdpb.RegionHeartbeatRequest) {
	c.regionCh <- request
}

func (c *client) SetRegionHeartbeatResponseHandler(h func(*pdpb.RegionHeartbeatResponse)) {
	if h == nil {
		h = func(*pdpb.RegionHeartbeatResponse) {}
	}
	c.heartbeatHandler.Store(h)
}

func (c *client) requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: c.clusterID,
	}
}
