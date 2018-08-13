package tikv

import (
	"fmt"
	"math"

	"github.com/juju/errors"
	"github.com/ngaut/faketikv/rowcodec"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	mockpkg "github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tipb/go-tipb"
)

// tryBuildClosureExecutor tries to build a closureExecutor for the DAGRequest.
// currently, only 'count(*)' is supported, but we can support all kinds of requests in the future.
func (svr *Server) tryBuildClosureExecutor(dagCtx *dagContext, dagReq *tipb.DAGRequest) (*closureExecutor, error) {
	ce, err := svr.newClosureExecutor(dagCtx, dagReq)
	if err != nil {
		return nil, errors.Trace(err)
	}
	executors := dagReq.Executors
	scanExec := executors[0]
	if scanExec.Tp == tipb.ExecType_TypeTableScan {
		ce.processFunc = ce.tableScanProcess
	} else {
		ce.processFunc = ce.indexScanProcess
	}
	ce.finishFunc = ce.scanFinish
	if len(executors) == 1 {
		return ce, nil
	}
	secondExec := executors[1]
	switch secondExec.Tp {
	case tipb.ExecType_TypeStreamAgg:
		return svr.tryBuildAggClosureExecutor(ce, executors)
	case tipb.ExecType_TypeLimit:
		ce.limit = int(secondExec.Limit.Limit)
		return ce, nil
	case tipb.ExecType_TypeSelection:
		ce.selectionCtx.conditions, err = convertToExprs(ce.sc, ce.fieldTps, secondExec.Selection.Conditions)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(executors) > 2 {
			if executors[2].Tp != tipb.ExecType_TypeLimit {
				return nil, nil
			}
			ce.limit = int(executors[2].Limit.Limit)
		}
		ce.processFunc = ce.selectionProcess
		return ce, nil
	default:
		return nil, nil
	}
}

func (svr *Server) newClosureExecutor(dagCtx *dagContext, dagReq *tipb.DAGRequest) (*closureExecutor, error) {
	e := &closureExecutor{
		evalContext: dagCtx.evalCtx,
		reqCtx:      dagCtx.reqCtx,
		outputOff:   dagReq.OutputOffsets,
		mvccStore:   svr.mvccStore,
		startTS:     dagReq.StartTs,
		limit:       math.MaxInt64,
	}
	seCtx := mockpkg.NewContext()
	seCtx.GetSessionVars().StmtCtx = e.sc
	e.seCtx = seCtx
	executors := dagReq.Executors
	scanExec := executors[0]
	switch scanExec.Tp {
	case tipb.ExecType_TypeTableScan:
		tblScan := executors[0].TblScan
		e.unique = true
		e.scanCtx.desc = tblScan.Desc
	case tipb.ExecType_TypeIndexScan:
		idxScan := executors[0].IdxScan
		e.unique = idxScan.GetUnique()
		e.scanCtx.desc = idxScan.Desc
		e.initIdxScanCtx()
	default:
		panic(fmt.Sprintf("unknown first executor type %s", executors[0].Tp))
	}
	ranges, err := svr.extractKVRanges(dagCtx.reqCtx.regCtx, dagCtx.keyRanges, e.scanCtx.desc)
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.kvRanges = ranges
	e.scanCtx.chk = chunk.NewChunkWithCapacity(e.fieldTps, 32)
	if e.idxScanCtx == nil {
		e.scanCtx.decoder, err = e.evalContext.newRowDecoder()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return e, nil
}

func (e *closureExecutor) initIdxScanCtx() {
	e.idxScanCtx = new(idxScanCtx)
	e.idxScanCtx.columnLen = len(e.columnInfos)
	e.idxScanCtx.pkStatus = pkColNotExists
	lastColumn := e.columnInfos[len(e.columnInfos)-1]
	// The PKHandle column info has been collected in ctx.
	if lastColumn.GetPkHandle() {
		if mysql.HasUnsignedFlag(uint(lastColumn.GetFlag())) {
			e.idxScanCtx.pkStatus = pkColIsUnsigned
		} else {
			e.idxScanCtx.pkStatus = pkColIsSigned
		}
		e.idxScanCtx.columnLen--
	} else if lastColumn.ColumnId == model.ExtraHandleID {
		e.idxScanCtx.pkStatus = pkColIsSigned
		e.idxScanCtx.columnLen--
	}
}

func (svr *Server) isCountAgg(pbAgg *tipb.Aggregation) bool {
	if len(pbAgg.AggFunc) == 1 && len(pbAgg.GroupBy) == 0 {
		aggFunc := pbAgg.AggFunc[0]
		if aggFunc.Tp == tipb.ExprType_Count && len(aggFunc.Children) == 1 {
			return true
		}
	}
	return false
}

func (svr *Server) tryBuildAggClosureExecutor(e *closureExecutor, executors []*tipb.Executor) (*closureExecutor, error) {
	if len(executors) > 2 {
		return nil, nil
	}
	agg := executors[1].Aggregation
	if !svr.isCountAgg(agg) {
		return nil, nil
	}
	child := agg.AggFunc[0].Children[0]
	switch child.Tp {
	case tipb.ExprType_ColumnRef:
		_, idx, err := codec.DecodeInt(child.Val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.aggCtx.colIdx = int(idx)
		e.processFunc = e.countColumnProcess
	default:
		e.processFunc = e.countStarProcess
	}
	e.finishFunc = e.countFinish
	return e, nil
}

// closureExecutor is an execution engine that flatten the DAGRequest.Executors to a single closure `processFunc` that
// process key/value pairs. We can define many closures for different kinds of requests, try to use the specially
// optimized one for some frequently used query.
type closureExecutor struct {
	*evalContext
	reqCtx       *requestCtx
	outputOff    []uint32
	mvccStore    *MVCCStore
	seCtx        sessionctx.Context
	kvRanges     []kv.KeyRange
	startTS      uint64
	ignoreLock   bool
	lockChecked  bool
	scanCtx      scanCtx
	idxScanCtx   *idxScanCtx
	selectionCtx selectionCtx
	aggCtx       aggCtx

	rowCount int
	unique   bool
	limit    int

	oldChunks   []tipb.Chunk
	oldRowBuf   []byte
	processFunc func(key, val []byte) error
	finishFunc  func() error
}

type scanCtx struct {
	count   int
	limit   int
	chk     *chunk.Chunk
	desc    bool
	decoder *rowcodec.XRowDecoder
}

type idxScanCtx struct {
	pkStatus  int
	columnLen int
}

type aggCtx struct {
	colIdx int
}

type selectionCtx struct {
	conditions []expression.Expression
}

func (e *closureExecutor) execute() ([]tipb.Chunk, error) {
	err := e.checkRangeLock()
	if err != nil {
		return nil, errors.Trace(err)
	}
	dbReader := e.reqCtx.getDBReader()
	for _, ran := range e.kvRanges {
		if e.unique && ran.IsPoint() {
			val, err := dbReader.Get(ran.StartKey, e.startTS)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(val) == 0 {
				continue
			}
			err = e.processFunc(ran.StartKey, val)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			if e.scanCtx.desc {
				err = dbReader.ReverseScan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e.processFunc)
			} else {
				err = dbReader.Scan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e.processFunc)
			}
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if e.rowCount == e.limit {
			break
		}
	}
	err = e.finishFunc()
	return e.oldChunks, err
}

func (e *closureExecutor) checkRangeLock() error {
	if !e.ignoreLock && !e.lockChecked {
		for _, ran := range e.kvRanges {
			err := e.mvccStore.CheckRangeLock(e.startTS, ran.StartKey, ran.EndKey)
			if err != nil {
				return err
			}
		}
		e.lockChecked = true
	}
	return nil
}

// countStarProcess is used for `count(*)`.
func (e *closureExecutor) countStarProcess(key, value []byte) error {
	e.rowCount++
	return nil
}

// countFinish is used for `count(*)`.
func (e *closureExecutor) countFinish() error {
	d := types.NewIntDatum(int64(e.rowCount))
	rowData, err := codec.EncodeValue(e.sc, nil, d)
	if err != nil {
		return errors.Trace(err)
	}
	e.oldChunks = appendRow(e.oldChunks, rowData, 0)
	return nil
}

func (e *closureExecutor) countColumnProcess(key, value []byte) error {
	if e.idxScanCtx != nil {
		values, _, err := tablecodec.CutIndexKeyNew(key, e.idxScanCtx.columnLen)
		if err != nil {
			return errors.Trace(err)
		}
		if values[0][0] != codec.NilFlag {
			e.rowCount++
		}
	} else {
		// Since the handle value doesn't affect the count result, we don't need to decode the handle.
		e.scanCtx.chk.Reset()
		err := e.scanCtx.decoder.Decode(value, 0, e.scanCtx.chk)
		if err != nil {
			return errors.Trace(err)
		}
		row := e.scanCtx.chk.GetRow(0)
		if !row.IsNull(e.aggCtx.colIdx) {
			e.rowCount++
		}
	}
	return nil
}

func (e *closureExecutor) tableScanProcess(key, value []byte) error {
	if e.rowCount == e.limit {
		return ScanBreak
	}
	e.rowCount++
	err := e.tableScanProcessCore(key, value)
	if e.scanCtx.chk.NumRows() == chunkMaxRows {
		err = e.chunkToOldChunk(e.scanCtx.chk)
	}
	return err
}

func (e *closureExecutor) tableScanProcessCore(key, value []byte) error {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return errors.Trace(err)
	}
	err = e.scanCtx.decoder.Decode(value, handle, e.scanCtx.chk)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *closureExecutor) scanFinish() error {
	return e.chunkToOldChunk(e.scanCtx.chk)
}

func (e *closureExecutor) indexScanProcess(key, value []byte) error {
	if e.rowCount == e.limit {
		return ScanBreak
	}
	e.rowCount++
	err := e.indexScanProcessCore(key, value)
	if e.scanCtx.chk.NumRows() == chunkMaxRows {
		err = e.chunkToOldChunk(e.scanCtx.chk)
	}
	return err
}

func (e *closureExecutor) indexScanProcessCore(key, value []byte) error {
	colLen := e.idxScanCtx.columnLen
	pkStatus := e.idxScanCtx.pkStatus
	chk := e.scanCtx.chk
	values, b, err := tablecodec.CutIndexKeyNew(key, colLen)
	if err != nil {
		return errors.Trace(err)
	}
	decoder := codec.NewDecoder(chk, e.sc.TimeZone)
	for i, colVal := range values {
		_, err = decoder.DecodeOne(colVal, i, e.fieldTps[i])
		if err != nil {
			return errors.Trace(err)
		}
	}
	if len(b) > 0 {
		if pkStatus != pkColNotExists {
			_, err = decoder.DecodeOne(b, colLen, e.fieldTps[colLen])
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else if pkStatus != pkColNotExists {
		handle, err := decodeHandle(value)
		if err != nil {
			return errors.Trace(err)
		}
		chk.AppendInt64(colLen, handle)
	}
	return nil
}

func (e *closureExecutor) chunkToOldChunk(chk *chunk.Chunk) error {
	var oldRow []types.Datum
	for i := 0; i < chk.NumRows(); i++ {
		oldRow = oldRow[:0]
		for _, outputOff := range e.outputOff {
			d := chk.GetRow(i).GetDatum(int(outputOff), e.fieldTps[outputOff])
			oldRow = append(oldRow, d)
		}
		var err error
		e.oldRowBuf, err = codec.EncodeValue(e.sc, e.oldRowBuf[:0], oldRow...)
		if err != nil {
			return errors.Trace(err)
		}
		e.oldChunks = appendRow(e.oldChunks, e.oldRowBuf, i)
	}
	chk.Reset()
	return nil
}

func (e *closureExecutor) selectionProcess(key, value []byte) error {
	if e.rowCount == e.limit {
		return ScanBreak
	}
	var err error
	if e.idxScanCtx != nil {
		err = e.indexScanProcessCore(key, value)
	} else {
		err = e.tableScanProcessCore(key, value)
	}
	if err != nil {
		return errors.Trace(err)
	}
	chk := e.scanCtx.chk
	row := chk.GetRow(chk.NumRows() - 1)
	ok := true
	for _, expr := range e.selectionCtx.conditions {
		i, _, err := expr.EvalInt(e.seCtx, row)
		if err != nil {
			return errors.Trace(err)
		}
		if i == 0 {
			ok = false
			chk.TruncateTo(chk.NumRows() - 1)
			break
		}
	}
	if ok {
		e.rowCount++
		if e.scanCtx.chk.NumRows() == chunkMaxRows {
			err = e.chunkToOldChunk(e.scanCtx.chk)
		}
	}
	return err
}
