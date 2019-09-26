package tikv

import (
	"bufio"
	"context"
	"encoding/binary"
	"net"
	"reflect"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/coocood/badger"
	"github.com/ngaut/log"
	"github.com/ngaut/unistore/config"
	"github.com/ngaut/unistore/rowcodec"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/ngaut/unistore/util"
)

type Packet struct {
	RequestHeader
	ReqBuf []byte
}

type ReqBatch struct {
	packets  []*Packet
	txn      *badger.Txn
	dbReader *dbreader.DBReader
	writeBuf []byte
	alloc    *Allocator
}

func (rb *ReqBatch) WriteU32(u uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], u)
	rb.writeBuf = append(rb.writeBuf, buf[:]...)
}

func (rb *ReqBatch) WriteBytes(data []byte) {
	rb.writeBuf = append(rb.writeBuf, data...)
}

func (rb *ReqBatch) Reset() {
	for i := range rb.packets {
		rb.packets[i] = nil
	}
	rb.packets = rb.packets[:0]
	rb.dbReader.Close()
	rb.txn = nil
	rb.dbReader = nil
	rb.writeBuf = rb.writeBuf[:0]
	rb.alloc.Reset()
}

type RequestHeader struct {
	RequestID     uint32
	RequestLen    uint32
	RegionID      uint64
	StoreID       uint32
	RegionVer     uint32
	RegionConfVer uint32
	Term          uint32
	Flags         uint32
	ReqType       uint32
}

const (
	ReqTypeGet uint32 = 1
)

type RespHeader struct {
	RequestID uint32
	Status    uint32
	RespLen   uint32
}

const (
	StatusOK        = 0
	StatusRegionErr = 1
	StatusKeyErr    = 2
)

func (h *RespHeader) ToBytes() []byte {
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = respHeaderSize
	hdr.Cap = respHeaderSize
	hdr.Data = uintptr(unsafe.Pointer(h))
	return b
}

type TurboServer struct {
	mvccStore     *MVCCStore
	regionManager RegionManager
	innerServer   InnerServer
	addr          string
}

type TurboConn struct {
	conn     net.Conn
	writeCh  chan *ReqBatch
	batchBuf chan *ReqBatch
	server   *TurboServer
	reader   *bufio.Reader
}

func NewTurboServer(rm RegionManager, store *MVCCStore, innerServer InnerServer, addr string) *TurboServer {
	return &TurboServer{
		mvccStore:     store,
		regionManager: rm,
		innerServer:   innerServer,
		addr:          addr,
	}
}

func (bs *TurboServer) Run() {
	var lc net.ListenConfig
	lc.Control = func(network, address string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) {
			_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, 8*1024*1024)
			_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 8*1024*1024)
		})
	}
	l, err := lc.Listen(context.Background(), "tcp", util.TurboAddr(bs.addr))
	if err != nil {
		panic(err)
	}
	turboConcurrency := config.Global.Turbo.Concurrency
	for {
		con, err := l.Accept()
		if err != nil {
			log.Error(err)
			return
		}
		tcpConn := con.(*net.TCPConn)
		fd, _ := tcpConn.File()
		sendBufSize, _ := syscall.GetsockoptInt(int(fd.Fd()), syscall.SOL_SOCKET, syscall.SO_SNDBUF)
		receiveBufSize, _ := syscall.GetsockoptInt(int(fd.Fd()), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
		log.Infof("connection send buffer %d, receive buffer %d", sendBufSize, receiveBufSize)
		batchBuf := make(chan *ReqBatch, turboConcurrency)
		for i := 0; i < turboConcurrency; i++ {
			batchBuf <- &ReqBatch{alloc: new(Allocator)}
		}
		turboConn := &TurboConn{
			conn:     con,
			writeCh:  make(chan *ReqBatch, turboConcurrency),
			batchBuf: batchBuf,
			server:   bs,
			reader:   bufio.NewReader(con),
		}
		go turboConn.readLoop()
		go turboConn.writeLoop()
	}
}

func (bs *TurboConn) readLoop() {
	var peekLen = 8
	for {
		_, err := bs.reader.Peek(peekLen)
		if err != nil {
			log.Error(err)
			return
		}
		batch := <-bs.batchBuf
		peekLen = readPackets(bs.reader, batch)
		if len(batch.packets) == 0 {
			bs.batchBuf <- batch
			continue
		}
		batch.txn = bs.server.mvccStore.db.NewTransaction(false)
		batch.dbReader = dbreader.NewDBReader(nil, nil, batch.txn, atomic.LoadUint64(&bs.server.mvccStore.safePoint.timestamp))
		go bs.handlePackets(batch)
	}
}

var endian = binary.LittleEndian

const (
	reqHeaderSize  = int(unsafe.Sizeof(RequestHeader{}))
	respHeaderSize = int(unsafe.Sizeof(RespHeader{}))
)

func readPackets(reader *bufio.Reader, batch *ReqBatch) int {
	for {
		if reader.Buffered() < reqHeaderSize {
			return reqHeaderSize
		}
		header, _ := reader.Peek(reqHeaderSize)
		p := batch.alloc.AllocPacket()
		p.RequestHeader = *(*RequestHeader)(unsafe.Pointer(&header[0]))
		if reader.Buffered() < int(p.RequestLen)+reqHeaderSize {
			return int(p.RequestLen) + reqHeaderSize
		}
		_, _ = reader.Discard(reqHeaderSize)
		p.ReqBuf = batch.alloc.AllocData(int(p.RequestLen))
		_, _ = reader.Read(p.ReqBuf)
		batch.packets = append(batch.packets, p)
	}
}

func (bs *TurboConn) handlePackets(batch *ReqBatch) {
	for _, packet := range batch.packets {
		batch.WriteU32(packet.RequestID)
		switch packet.ReqType {
		case ReqTypeGet:
			respData := bs.handleGet(batch, packet)
			batch.WriteU32(uint32(len(respData)))
			batch.WriteBytes(respData)
		default:
			log.Warn("unknown type", packet.ReqType)
		}
	}
	bs.writeCh <- batch
}

func (bs *TurboConn) handleGet(batch *ReqBatch, packet *Packet) (respData []byte) {
	_, pbErr := bs.server.regionManager.GetRegionByReqHeader(&packet.RequestHeader)
	if pbErr != nil {
		batch.WriteU32(StatusRegionErr)
		respData, _ = pbErr.Marshal()
		return
	}
	ts := endian.Uint64(packet.ReqBuf)
	key := packet.ReqBuf[8:]
	err := bs.server.mvccStore.CheckKeysLock(ts, key)
	if err != nil {
		batch.WriteU32(StatusKeyErr)
		respData, _ = convertToKeyError(err).Marshal()
		return
	}
	respData, err = batch.dbReader.Get(packet.ReqBuf[8:], endian.Uint64(packet.ReqBuf))
	if err != nil {
		batch.WriteU32(StatusKeyErr)
		respData, _ = convertToKeyError(err).Marshal()
		return
	}
	batch.WriteU32(StatusOK)
	if rowcodec.IsRowKey(key) {
		respData, err = rowcodec.RowToOldRow(respData, nil)
		if err != nil {
			panic(err)
		}
	}
	return
}

func (bs *TurboConn) writeLoop() {
	for batch := range bs.writeCh {
		_, err := bs.conn.Write(batch.writeBuf)
		if err != nil {
			log.Error(err)
			return
		}
		batch.Reset()
		bs.batchBuf <- batch
	}
}

type Allocator struct {
	off  int
	data []byte
}

func (a *Allocator) AllocPacket() *Packet {
	data := a.AllocData(int(unsafe.Sizeof(Packet{})))
	return (*Packet)(unsafe.Pointer(&data[0]))
}

func (a *Allocator) Reset() {
	allocated := a.data[:a.off]
	for i := range allocated {
		allocated[i] = 0
	}
	a.off = 0
}

func (a *Allocator) AllocData(size int) []byte {
	if a.off+size > len(a.data) {
		newSize := 256 * 1024
		if newSize < size {
			newSize = size
		}
		a.data = make([]byte, newSize)
		a.off = 0
	}
	data := a.data[a.off : a.off+size]
	a.off += size
	return data
}
