package deadlock

import (
	"fmt"
	"sync"
)

type Detector struct {
	waitForMap map[uint64]*txnList
	lock       sync.Mutex
}

type txnList struct {
	txns []txnKeyHashPair
}

type txnKeyHashPair struct {
	txn     uint64
	keyHash uint64
}

func NewDetector() *Detector {
	return &Detector{
		waitForMap: map[uint64]*txnList{},
	}
}

type ErrDeadlock struct {
	keyHash uint64
}

func (e *ErrDeadlock) Error() string {
	return fmt.Sprintf("deadlock(%d)", e.keyHash)
}

func (d *Detector) Detect(sourceTxn, waitForTxn, keyHash uint64) error {
	d.lock.Lock()
	err := d.doDetect(sourceTxn, waitForTxn)
	if err == nil {
		d.register(sourceTxn, waitForTxn, keyHash)
	}
	d.lock.Unlock()
	return err
}

func (d *Detector) doDetect(sourceTxn, waitForTxn uint64) error {
	list := d.waitForMap[waitForTxn]
	if list == nil {
		return nil
	}
	for _, nextTarget := range list.txns {
		if nextTarget.txn == sourceTxn {
			return &ErrDeadlock{keyHash: nextTarget.keyHash}
		}
		if err := d.doDetect(sourceTxn, nextTarget.txn); err != nil {
			return err
		}
	}
	return nil
}

func (d *Detector) register(sourceTxn, waitForTxn, keyHash uint64) {
	list := d.waitForMap[sourceTxn]
	pair := txnKeyHashPair{txn: waitForTxn, keyHash: keyHash}
	if list == nil {
		d.waitForMap[sourceTxn] = &txnList{txns: []txnKeyHashPair{pair}}
		return
	}
	for _, tar := range list.txns {
		if tar.txn == waitForTxn && tar.keyHash == keyHash {
			return
		}
	}
	list.txns = append(list.txns, pair)
}

func (d *Detector) CleanUp(txn uint64) {
	d.lock.Lock()
	delete(d.waitForMap, txn)
	d.lock.Unlock()
}

func (d *Detector) CleanUpWaitFor(txn, waitForTxn, keyHash uint64) {
	pair := txnKeyHashPair{txn: waitForTxn, keyHash: keyHash}
	d.lock.Lock()
	l := d.waitForMap[txn]
	if l != nil {
		for i, tar := range l.txns {
			if tar == pair {
				l.txns = append(l.txns[:i], l.txns[i+1:]...)
				break
			}
		}
		if len(l.txns) == 0 {
			delete(d.waitForMap, txn)
		}
	}
	d.lock.Unlock()

}
