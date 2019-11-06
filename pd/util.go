package pd

import (
	"context"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"google.golang.org/grpc"
)

func connectToEndpoints(ctx context.Context, eps []string) (*grpc.ClientConn, *pdpb.GetMembersResponse, error) {
	set := make(map[string]struct{}, len(eps))

	var members *pdpb.GetMembersResponse
	var clusterID uint64

	for _, ep := range eps {
		if _, ok := set[ep]; ok {
			return nil, nil, errors.Errorf("duplicate PD endpoint %s", ep)
		}
		set[ep] = struct{}{}

		cc, resp, err := connect(ctx, ep)
		if err != nil {
			// Ignore failed PD node.
			log.Infof("PD endpoint %s failed to respond, err: %s", ep, err)
			continue
		}
		cc.Close()

		cid := resp.GetHeader().GetClusterId()
		if clusterID != 0 {
			if cid != uint64(clusterID) {
				return nil, nil, errors.Errorf("PD response clusterID mismatch, want %d, got %d", clusterID, cid)
			}
		} else {
			clusterID = cid
		}

		if members == nil {
			members = resp
		}
	}

	if members == nil {
		return nil, nil, errors.New("PD cluster failed to respond")
	}

	return tryConnectLeader(ctx, members)
}

func tryConnectLeader(ctx context.Context, previous *pdpb.GetMembersResponse) (*grpc.ClientConn, *pdpb.GetMembersResponse, error) {
	prevLeader := previous.GetLeader()
	members := previous.GetMembers()
	clusterID := previous.GetHeader().GetClusterId()
	var resp *pdpb.GetMembersResponse

	// Try to connect to other members, then the previous leader.
	for _, m := range members {
		if m.GetMemberId() == prevLeader.GetMemberId() {
			continue
		}
		if resp = connectToMember(ctx, clusterID, m); resp != nil {
			break
		}
	}
	if resp == nil {
		resp = connectToMember(ctx, clusterID, prevLeader)
	}

	if resp != nil {
		leader := resp.GetLeader()
		log.Infof("get leader: %v", leader)
		for _, ep := range leader.GetClientUrls() {
			if cc, _, err := connect(ctx, ep); err == nil {
				log.Infof("connected to PD leader %s", ep)
				return cc, resp, nil
			}
		}
	}

	return nil, nil, errors.Errorf("failed to connect to %v", members)
}

func connectToMember(ctx context.Context, clusterID uint64, member *pdpb.Member) *pdpb.GetMembersResponse {
	for _, ep := range member.GetClientUrls() {
		log.Infof("connect to endpoint: %s", ep)
		cc, resp, err := connect(ctx, ep)
		if err != nil {
			log.Warnf("connect endpoint %s failed, err: %s", ep, err)
			continue
		}
		cc.Close()
		cid := resp.GetHeader().GetClusterId()
		if cid != clusterID {
			panic(errors.Errorf("%s no longer belongs to cluster %d, it is in %d", ep, clusterID, cid))
		}
		return resp
	}
	return nil
}

func connect(ctx context.Context, ep string) (*grpc.ClientConn, *pdpb.GetMembersResponse, error) {
	addr := strings.TrimPrefix(ep, "http://")
	cc, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	client := pdpb.NewPDClient(cc)
	req := new(pdpb.GetMembersRequest)
	resp, err := client.GetMembers(ctx, req)
	if err != nil {
		cc.Close()
	}
	return cc, resp, err
}
