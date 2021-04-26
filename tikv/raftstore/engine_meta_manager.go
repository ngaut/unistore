package raftstore

import (
	"encoding/binary"
	"github.com/pingcap/badger/protos"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/kvproto/pkg/metapb"
	"sync"
)

// MetaChangeListener implements the badger.MetaChangeListener interface.
type MetaChangeListener struct {
	mu    sync.Mutex
	msgCh chan<- Msg
	queue []Msg
}

func NewMetaChangeListener() *MetaChangeListener {
	return &MetaChangeListener{}
}

// OnChange implements the badger.MetaChangeListener interface.
func (l *MetaChangeListener) OnChange(e *protos.ShardChangeSet) {
	y.Assert(e.ShardID != 0)
	msg := NewPeerMsg(MsgTypeGenerateEngineChangeSet, e.ShardID, e)
	l.mu.Lock()
	ch := l.msgCh
	if ch == nil {
		l.queue = append(l.queue, msg)
	}
	l.mu.Unlock()
	if ch != nil {
		ch <- msg
	}
}

func (l *MetaChangeListener) initMsgCh(msgCh chan<- Msg) {
	l.mu.Lock()
	l.msgCh = msgCh
	queue := l.queue
	l.mu.Unlock()
	for _, msg := range queue {
		msgCh <- msg
	}
}

type snapData struct {
	region    *metapb.Region
	changeSet *protos.ShardChangeSet
	maxReadTS uint64
}

func (sd *snapData) Marshal() []byte {
	regionData, _ := sd.region.Marshal()
	changeData, _ := sd.changeSet.Marshal()
	buf := make([]byte, 0, 4+len(regionData)+4+len(changeData))
	buf = appendSlice(buf, regionData)
	buf = appendSlice(buf, changeData)
	buf = appendU64(buf, sd.maxReadTS)
	return buf
}

func (sd *snapData) Unmarshal(data []byte) error {
	sd.region = new(metapb.Region)
	element, data := cutSlices(data)
	err := sd.region.Unmarshal(element)
	if err != nil {
		return err
	}
	sd.changeSet = new(protos.ShardChangeSet)
	element, data = cutSlices(data)
	err = sd.changeSet.Unmarshal(element)
	if err != nil {
		return err
	}
	sd.maxReadTS = binary.LittleEndian.Uint64(data)
	return nil
}

func appendSlice(buf []byte, element []byte) []byte {
	buf = append(buf, make([]byte, 4)...)
	binary.LittleEndian.PutUint32(buf[len(buf)-4:], uint32(len(element)))
	return append(buf, element...)
}

func appendU64(buf []byte, v uint64) []byte {
	tmp := make([]byte, 8)
	binary.LittleEndian.PutUint64(tmp, v)
	return append(buf, tmp...)
}

func cutSlices(data []byte) (element []byte, remain []byte) {
	length := binary.LittleEndian.Uint32(data)
	data = data[4:]
	return data[:length], data[length:]
}
