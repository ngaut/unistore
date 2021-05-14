package sdb

import (
	"encoding/binary"
	"github.com/ngaut/unistore/sdb/table/sstable"
	"github.com/pingcap/badger/options"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
)

type l0Table struct {
	cfs      []*sstable.Table
	fid      uint64
	filename string
	size     int64
	commitTS uint64
}

func (st *l0Table) Delete() error {
	return os.Remove(st.filename)
}

func (st *l0Table) getSplitIndex(splitKeys [][]byte) int {
	for _, cf := range st.cfs {
		if cf != nil {
			return getSplitShardIndex(splitKeys, cf.Smallest().UserKey)
		}
	}
	return 0
}

func openL0Table(filename string, fid uint64) (*l0Table, error) {
	shardData, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	l0 := &l0Table{
		fid:      fid,
		filename: filename,
		size:     int64(len(shardData)),
	}
	l0.commitTS = binary.LittleEndian.Uint64(shardData[len(shardData)-8:])
	numCF := int(shardData[len(shardData)-9])
	shardData = shardData[:len(shardData)-9]
	l0.cfs = make([]*sstable.Table, 0, numCF)
	cfIdx := shardData[len(shardData)-numCF*8:]
	for i := 0; i < len(cfIdx); i += 8 {
		dataStartOff := uint32(0)
		if i != 0 {
			dataStartOff = binary.LittleEndian.Uint32(cfIdx[i-4:])
		}
		dataEndOff := binary.LittleEndian.Uint32(cfIdx[i:])
		data := shardData[dataStartOff:dataEndOff]
		if len(data) == 0 {
			l0.cfs = append(l0.cfs, nil)
			continue
		}
		idxEndOff := binary.LittleEndian.Uint32(cfIdx[i+4:])
		idxData := shardData[dataEndOff:idxEndOff]
		inMemFile := sstable.NewInMemFile(data, idxData)
		tbl, err := sstable.OpenInMemoryTable(inMemFile)
		if err != nil {
			return nil, err
		}
		l0.cfs = append(l0.cfs, tbl)
	}
	return l0, nil
}

func (sl0 *l0Table) Get(cf int, key y.Key, keyHash uint64) y.ValueStruct {
	tbl := sl0.cfs[cf]
	if tbl == nil {
		return y.ValueStruct{}
	}
	v, err := tbl.Get(key, keyHash)
	if err != nil {
		// TODO: handle error
		log.Error("get data in table failed", zap.Error(err))
	}
	return v
}

func (sl0 *l0Table) newIterator(cf int, reverse bool) y.Iterator {
	tbl := sl0.cfs[cf]
	if tbl == nil {
		return nil
	}
	return tbl.NewIterator(reverse)
}

type l0Tables struct {
	tables []*l0Table
}

func (sl0s *l0Tables) totalSize() int64 {
	var size int64
	for _, tbl := range sl0s.tables {
		size += tbl.size
	}
	return size
}

type l0Builder struct {
	builders []*sstable.Builder
	commitTS uint64
}

func newL0Builder(numCFs int, opt options.TableBuilderOptions, commitTS uint64) *l0Builder {
	sdb := &l0Builder{
		builders: make([]*sstable.Builder, numCFs),
		commitTS: commitTS,
	}
	for i := 0; i < numCFs; i++ {
		sdb.builders[i] = sstable.NewTableBuilder(nil, nil, 0, opt)
	}
	return sdb
}

func (e *l0Builder) Add(cf int, key y.Key, value y.ValueStruct) {
	e.builders[cf].Add(key, value)
}

/*
 L0 Data format:
 | CF0 Data | CF0 index | CF1 Data | CF1 index | cfs index | numCF |
*/
func (e *l0Builder) Finish() []byte {
	cfDatas := make([][]byte, 0, len(e.builders)*2)
	cfsIndex := make([]byte, len(e.builders)*8)
	var fileSize int
	for i, builder := range e.builders {
		result, _ := builder.Finish()
		cfDatas = append(cfDatas, result.FileData)
		fileSize += len(result.FileData)
		binary.LittleEndian.PutUint32(cfsIndex[i*8:], uint32(fileSize))
		cfDatas = append(cfDatas, result.IndexData)
		fileSize += len(result.IndexData)
		binary.LittleEndian.PutUint32(cfsIndex[i*8+4:], uint32(fileSize))
	}
	result := make([]byte, 0, fileSize+len(cfsIndex)+1+8)
	result = append(result)
	for _, cfData := range cfDatas {
		result = append(result, cfData...)
	}
	result = append(result, cfsIndex...)
	result = append(result, byte(len(e.builders))) // number of CF
	commitBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(commitBuf, e.commitTS)
	result = append(result, commitBuf...)
	return result
}
