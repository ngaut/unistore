package sdb

import (
	"github.com/ngaut/unistore/s3util"
	"github.com/ngaut/unistore/sdb/table/sstable"
	"github.com/ngaut/unistore/sdbpb"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"os"
	"sort"
	"sync/atomic"
	"unsafe"
)

type IngestTree struct {
	ChangeSet *sdbpb.ChangeSet
	MaxTS     uint64
	LocalPath string
	Passive   bool
}

func (sdb *DB) Ingest(ingestTree *IngestTree) error {
	if shd := sdb.GetShard(ingestTree.ChangeSet.ShardID); shd != nil {
		return errors.New("shard already exists")
	}
	sdb.orc.Lock()
	if sdb.orc.nextCommit < ingestTree.MaxTS {
		sdb.orc.nextCommit = ingestTree.MaxTS
	}
	sdb.orc.Unlock()
	defer sdb.orc.doneCommit(ingestTree.MaxTS)
	guard := sdb.resourceMgr.Acquire()
	defer guard.Done()

	err := sdb.loadFiles(ingestTree)
	if err != nil {
		return err
	}
	l0s, levelHandlers, err := sdb.createIngestTreeLevelHandlers(ingestTree)
	if err != nil {
		return err
	}

	shard := newShardForIngest(ingestTree.ChangeSet, sdb.opt, sdb.metrics)
	for _, l0 := range l0s.tables {
		shard.addEstimatedSize(l0.Size())
	}
	shard.SetPassive(ingestTree.Passive)
	atomic.StorePointer(shard.memTbls, unsafe.Pointer(&memTables{}))
	atomic.StorePointer(shard.l0s, unsafe.Pointer(l0s))
	shard.foreachLevel(func(cf int, level *levelHandler) (stop bool) {
		shard.addEstimatedSize(level.totalSize)
		scf := shard.cfs[cf]
		y.Assert(scf.casLevelHandler(level.level, level, levelHandlers[cf][level.level-1]))
		return false
	})
	// Ingest is manually triggered with meta change, so we don't need to notify meta listener.
	if err = sdb.manifest.writeChangeSet(ingestTree.ChangeSet); err != nil {
		return err
	}
	sdb.shardMap.Store(shard.ID, shard)
	return nil
}

func (sdb *DB) createIngestTreeLevelHandlers(ingestTree *IngestTree) (*l0Tables, [][]*levelHandler, error) {
	l0s := &l0Tables{}
	newHandlers := make([][]*levelHandler, sdb.numCFs)
	for cf := 0; cf < sdb.numCFs; cf++ {
		for l := 1; l <= ShardMaxLevel; l++ {
			newHandler := newLevelHandler(sdb.opt.NumLevelZeroTablesStall, l, sdb.metrics)
			newHandlers[cf] = append(newHandlers[cf], newHandler)
		}
	}
	snap := ingestTree.ChangeSet.Snapshot
	for _, l0Create := range snap.L0Creates {
		l0Tbl, err := sstable.OpenL0Table(sstable.NewFilename(l0Create.ID, sdb.opt.Dir), l0Create.ID)
		if err != nil {
			return nil, nil, err
		}
		l0s.tables = append(l0s.tables, l0Tbl)
	}
	for _, tblCreate := range snap.TableCreates {
		handler := newHandlers[tblCreate.CF][tblCreate.Level-1]
		filename := sstable.NewFilename(tblCreate.ID, sdb.opt.Dir)
		reader, err := newTableFile(filename, sdb)
		if err != nil {
			return nil, nil, err
		}
		tbl, err := sstable.OpenTable(reader, sdb.blkCache)
		if err != nil {
			return nil, nil, err
		}
		handler.totalSize += tbl.Size()
		handler.tables = append(handler.tables, tbl)
	}
	sort.Slice(l0s.tables, func(i, j int) bool {
		return l0s.tables[i].CommitTS() > l0s.tables[j].CommitTS()
	})
	for cf := 0; cf < sdb.numCFs; cf++ {
		for l := 1; l <= ShardMaxLevel; l++ {
			handler := newHandlers[cf][l-1]
			sort.Slice(handler.tables, func(i, j int) bool {
				return handler.tables[i].Smallest().Compare(handler.tables[j].Smallest()) < 0
			})
		}
	}
	return l0s, newHandlers, nil
}

func (sdb *DB) loadFiles(ingestTree *IngestTree) error {
	snap := ingestTree.ChangeSet.Snapshot
	bt := s3util.NewBatchTasks()
	for i := range snap.L0Creates {
		l0 := snap.L0Creates[i]
		bt.AppendTask(func() error {
			if ingestTree.LocalPath != "" {
				return sdb.loadFileFromLocalPath(ingestTree.LocalPath, l0.ID)
			}
			return sdb.loadFileFromS3(l0.ID)
		})
	}
	if sdb.s3c != nil {
		if err := sdb.s3c.BatchSchedule(bt); err != nil {
			return err
		}
	}
	bt = s3util.NewBatchTasks()
	for i := range snap.TableCreates {
		tbl := snap.TableCreates[i]
		bt.AppendTask(func() error {
			if ingestTree.LocalPath != "" {
				return sdb.loadFileFromLocalPath(ingestTree.LocalPath, tbl.ID)
			}
			return sdb.loadFileFromS3(tbl.ID)
		})
	}
	if sdb.s3c != nil {
		if err := sdb.s3c.BatchSchedule(bt); err != nil {
			return err
		}
	}

	return nil
}

func (sdb *DB) loadFileFromS3(id uint64) error {
	localFileName := sstable.NewFilename(id, sdb.opt.Dir)
	_, err := os.Stat(localFileName)
	if err == nil {
		return nil
	}
	blockKey := sdb.s3c.BlockKey(id)
	tmpBlockFileName := localFileName + ".tmp"
	err = sdb.s3c.GetToFile(blockKey, tmpBlockFileName)
	if err != nil {
		return err
	}
	return os.Rename(tmpBlockFileName, localFileName)
}

func (sdb *DB) loadFileFromLocalPath(localPath string, id uint64) error {
	localFileName := sstable.NewFilename(id, localPath)
	dstFileName := sstable.NewFilename(id, sdb.opt.Dir)
	return os.Link(localFileName, dstFileName)
}
