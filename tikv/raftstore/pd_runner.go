package raftstore

import (
	"context"

	"github.com/coocood/badger"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/pd"
)

type pdRunner struct {
	storeID   uint64
	pdClient  pd.Client
	router    *router
	db        *badger.DB
	scheduler chan<- task
}

func newPDRunner(storeID uint64, pdClient pd.Client, router *router, db *badger.DB, scheduler chan<- task) *pdRunner {
	return &pdRunner{
		storeID:   storeID,
		pdClient:  pdClient,
		router:    router,
		db:        db,
		scheduler: scheduler,
	}
}

func (r *pdRunner) run(t task) {
	switch t.tp {
	case taskTypePDAskSplit:
		r.onAskSplit(t.data.(*pdAskSplitTask))
	case taskTypePDAskBatchSplit:
		r.onAskBatchSplit(t.data.(*pdAskBatchSplitTask))
	case taskTypePDHeartbeat:
		r.onHeartbeat(t.data.(*pdRegionHeartbeatTask))
	case taskTypePDStoreHeartbeat:
		r.onStoreHeartbeat(t.data.(*pdStoreHeartbeatTask))
	case taskTypePDReportBatchSplit:
		r.onReportBatchSplit(t.data.(*pdReportBatchSplitTask))
	case taskTypePDValidatePeer:
		r.onValidatePeer(t.data.(*pdValidatePeerTask))
	case taskTypePDReadStats:
		r.onReadStats(t.data.(*pdReadStatsTask))
	case taskTypePDDestroyPeer:
		r.onDestroyPeer(t.data.(*pdDestroyPeerTask))
	default:
		log.Error("unsupported task type:", t.tp)
	}
}

func (r *pdRunner) onAskSplit(t *pdAskSplitTask) {
	resp, err := r.pdClient.AskSplit(context.TODO(), t.region)
	if err != nil {
		log.Error(err)
	}
}

func (r *pdRunner) onAskBatchSplit(t *pdAskBatchSplitTask) {

}

func (r *pdRunner) onHeartbeat(t *pdRegionHeartbeatTask) {

}

func (r *pdRunner) onStoreHeartbeat(t *pdStoreHeartbeatTask) {

}

func (r *pdRunner) onReportBatchSplit(t *pdReportBatchSplitTask) {

}

func (r *pdRunner) onValidatePeer(t *pdValidatePeerTask) {

}

func (r *pdRunner) onReadStats(t *pdReadStatsTask) {

}

func (r *pdRunner) onDestroyPeer(t *pdDestroyPeerTask) {

}
