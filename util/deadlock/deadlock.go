package deadlock

import (
	"errors"
	"sync"
)

type Detector struct {
	txnMap map[uint64]*txnList
	lock   sync.Mutex
}

type txnList struct {
	txns []uint64
}

func NewDetector() *Detector {
	return &Detector{
		txnMap: map[uint64]*txnList{},
	}
}

var ErrDeadlock = errors.New("deadlock")

func (d *Detector) Detect(sourceTxn, targetTxn uint64) error {
	d.lock.Lock()
	err := d.doDetect(sourceTxn, targetTxn)
	if err == nil {
		d.register(sourceTxn, targetTxn)
	}
	d.lock.Unlock()
	return err
}

func (d *Detector) doDetect(sourceTxn, targetTxn uint64) error {
	list := d.txnMap[targetTxn]
	if list == nil {
		return nil
	}
	for _, nextTarget := range list.txns {
		if nextTarget == sourceTxn {
			return ErrDeadlock
		}
		if d.doDetect(sourceTxn, nextTarget) != nil {
			return ErrDeadlock
		}
	}
	return nil
}

func (d *Detector) register(sourceTxn, targetTxn uint64) {
	list := d.txnMap[sourceTxn]
	if list == nil {
		d.txnMap[sourceTxn] = &txnList{txns: []uint64{targetTxn}}
		return
	}
	for _, tar := range list.txns {
		if tar == targetTxn {
			return
		}
	}
	list.txns = append(list.txns, targetTxn)
}

func (d *Detector) CleanUp(txn uint64) {
	d.lock.Lock()
	delete(d.txnMap, txn)
	d.lock.Unlock()
}
