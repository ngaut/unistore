package tikv

import (
	"github.com/ngaut/unistore/pd"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/kvproto/pkg/tikvpb"
)

type InnerServer interface {
	Setup(pdClient pd.Client)
	Start(pdClient pd.Client) error
	Stop() error
	Raft(stream tikvpb.Tikv_RaftServer) error
	BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error
	Snapshot(stream tikvpb.Tikv_SnapshotServer) error
}

type StandAlongInnerServer struct {
	bundle *mvcc.DBBundle
}

func NewStandAlongInnerServer(bundle *mvcc.DBBundle) *StandAlongInnerServer {
	return &StandAlongInnerServer{
		bundle: bundle,
	}
}

func (is *StandAlongInnerServer) Raft(stream tikvpb.Tikv_RaftServer) error {
	return nil
}

func (is *StandAlongInnerServer) BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error {
	return nil
}

func (is *StandAlongInnerServer) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return nil
}

func (is *StandAlongInnerServer) Setup(pdClient pd.Client) {}

func (is *StandAlongInnerServer) Start(pdClient pd.Client) error {
	return nil
}

func (is *StandAlongInnerServer) Stop() error {
	return is.bundle.DB.Close()
}
