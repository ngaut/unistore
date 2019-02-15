package tikv

import (
	"fmt"
	"github.com/coocood/badger"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/codec"
	"golang.org/x/net/context"
	"math"
	"time"

	// remember the tablecodec has two different versions,
	// one is on the tibd/tablecodec/origin/tablecodec.go
	// and another is on tidb/tablecodec/tablecodec.go,
	// this must changes with the isShardingEnabled parameter.
	"github.com/pingcap/tidb/tablecodec/origin"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"os"
	"sync"
	"testing"
)

const (
	keyNumber = 3
	TableId   = 0
	START_TS  = 10
	TTL       = 60000
)

// parameter settings for unistore.
type settings struct {
	shardKey bool
	numDb    int
	dbPath   string
	vlogPath string
}

type data struct {
	encodedKVDatas []*encodedKVData
	colInfos       []*tipb.ColumnInfo
	rowValues      map[int64][]types.Datum
	colTypes       map[int64]*types.FieldType
}

type encodedKVData struct {
	encodedRowKey   []byte
	encodedRowValue []byte
}

type TestStore struct {
	mvccStore *MVCCStore
	svr       *Server
}

func NewTestStore() (testStore *TestStore, err error) {
	settings := settings{
		shardKey: false,
		numDb:    1,
		dbPath:   "/tmp/dbpath",
		vlogPath: "/tmp/vlogpath",
	}
	os.RemoveAll(settings.dbPath)
	os.Mkdir(settings.dbPath, 0700)
	os.RemoveAll(settings.vlogPath)
	os.Mkdir(settings.vlogPath, 0700)
	store, testStoreError := makeTestStore(settings)
	if testStoreError != nil {
		return nil, testStoreError
	}
	return store, nil
}

func (store *TestStore) InitTestData(encodedKVDatas []*encodedKVData) []error {
	reqCtx := requestCtx{
		regCtx: &regionCtx{
			latches: make(map[uint64]*sync.WaitGroup),
		},
		dbIdx: TableId,
		svr:   store.svr,
	}
	var i = 0
	for _, kvData := range encodedKVDatas {
		mutation := makeATestMutaion(kvrpcpb.Op_Put, kvData.encodedRowKey,
			kvData.encodedRowValue)
		errors := store.mvccStore.Prewrite(&reqCtx, []*kvrpcpb.Mutation{mutation,},
			kvData.encodedRowKey, uint64(START_TS+i), TTL)
		if errors != nil {
			return errors
		}
		commitErrors := make([]error, 0)
		commitError := store.mvccStore.Commit(&reqCtx, [][]byte{kvData.encodedRowKey},
			uint64(START_TS+i), uint64(START_TS+i+1))
		if commitError != nil {
			return append(commitErrors, commitError)
		}
		i += 2
	}
	return nil
}

func makeATestMutaion(op kvrpcpb.Op, key []byte, value []byte) *kvrpcpb.Mutation {
	return &kvrpcpb.Mutation{
		Op:    op,
		Key:   key,
		Value: value,
	}
}

func createTestDB(settings settings, idx int, safePoint *SafePoint) (db *badger.DB, err error) {
	subPath := fmt.Sprintf("/%d", idx)
	opts := badger.DefaultOptions
	opts.Dir = settings.dbPath + subPath
	opts.ValueDir = settings.vlogPath + subPath
	return badger.Open(opts)
}

func makeTestStore(settings settings) (testStore *TestStore, err error) {
	if settings.shardKey {
		EnableSharding()
	}
	safePoint := &SafePoint{}
	dbs := make([]*badger.DB, settings.numDb)
	for i := 0; i < settings.numDb; i++ {
		dbs[i], err = createTestDB(settings, i, safePoint)
		if err != nil {
			return nil, err
		}
	}
	store := NewMVCCStore(dbs, settings.dbPath, safePoint)
	svr := &Server{
		mvccStore: store,
	}
	return &TestStore{store, svr}, nil
}

func PrepareTestTableData(t *testing.T, keyNumber int, tableId int64) *data {
	stmtCtx := new(stmtctx.StatementContext)
	colIds := []int64{1, 2, 3}
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeDouble),
	}
	colInfos := make([]*tipb.ColumnInfo, 3)
	colTypeMap := map[int64]*types.FieldType{}
	for i := 0; i < 3; i++ {
		colInfos[i] = &tipb.ColumnInfo{
			ColumnId: colIds[i],
			Tp:       int32(colTypes[i].Tp),
		}
		colTypeMap[colIds[i]] = colTypes[i]
	}
	rowValues := map[int64][]types.Datum{}
	encodedKVDatas := make([]*encodedKVData, keyNumber)
	for i := 0; i < keyNumber; i++ {
		datum := types.MakeDatums(i, "abc", 10.0)
		rowValues[int64(i)] = datum
		rowEncodedData, err := tablecodec.EncodeRow(stmtCtx, datum,
			colIds, nil, nil)
		require.Nil(t, err)
		rowKeyEncodedData := tablecodec.EncodeRowKeyWithHandle(tableId, int64(i))
		encodedKVDatas[i] = &encodedKVData{encodedRowKey: rowKeyEncodedData, encodedRowValue: rowEncodedData}
	}
	return &data{
		colInfos:       colInfos,
		encodedKVDatas: encodedKVDatas,
		rowValues:      rowValues,
		colTypes:       colTypeMap,
	}
}

func getTestPointRange(tableId int64, handle int64) kv.KeyRange {
	startKey := tablecodec.EncodeRowKeyWithHandle(tableId, handle)
	endKey := make([]byte, len(startKey))
	copy(endKey, startKey)
	convertToPrefixNext(endKey)
	return kv.KeyRange{
		StartKey: startKey,
		EndKey:   endKey,
	}
}

// convert this key to the smallest key which is larger than the key given.
// see tikv/src/coprocessor/util.rs for more detail.
func convertToPrefixNext(key []byte) []byte {
	if key == nil || len(key) == 0 {
		key = make([]byte, 1)
		key[0] = 0
		return key
	}
	for i := len(key) - 1; i >= 0; i -= 1 {
		if key[i] == 255 {
			key[i] = 0
		} else {
			key[i] += 1
			return key
		}
	}
	for i := 0; i < len(key); i++ {
		key[i] = 255
	}
	return append(key, 0)
}

// return whether these two keys are equal.
func isPrefixNext(key []byte, expected []byte) bool {
	key = convertToPrefixNext(key)
	if len(key) != len(expected) {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] != expected[i] {
			return false
		}
	}
	return true
}

func executeAPointGet(data *data, store *TestStore, handler int64) (chunks []tipb.Chunk, err error) {
	// we would prepare the dagRequest and try build a executor as follows,
	// the reason that we don't make a tableScanExecutor directly here is
	// it would be replaced by closureExecutor in the future, so, for compatibility,
	// we just build a dagReq, and call buildDAGExecutor to build
	// the corresponding executor.
	aRange := getTestPointRange(TableId, handler)
	dagReq := &tipb.DAGRequest{
		StartTs: uint64(time.Now().UnixNano()),
		Executors: [] *tipb.Executor{
			{
				Tp: tipb.ExecType_TypeTableScan,
				TblScan: &tipb.TableScan{
					Columns: data.colInfos,
					TableId: TableId,
				},
			},
		},
		// only want first two cols.
		OutputOffsets: [] uint32{
			uint32(0),
			uint32(1),
		},
	}
	sc := flagsToStatementContext(dagReq.Flags)
	sc.TimeZone = time.FixedZone("UTC", int(dagReq.TimeZoneOffset))
	dagCtx := &dagContext{
		reqCtx: &requestCtx{
			svr:   store.svr,
			dbIdx: TableId,
			regCtx: &regionCtx{
				meta: &metapb.Region{
					StartKey: nil,
					EndKey:   nil,
				},
			},
		},
		dagReq: dagReq,
		keyRanges: []*coprocessor.KeyRange{
			{
				Start: aRange.StartKey,
				End:   aRange.EndKey,
			},
		},
		evalCtx: &evalContext{sc: sc},
	}
	dagCtx.evalCtx.setColumnInfo(dagReq.Executors[0].TblScan.Columns)
	closureExec, error := store.svr.tryBuildClosureExecutor(dagCtx, dagReq)
	if error != nil {
		return nil, error
	}
	var rowCnt int
	if closureExec != nil {
		chunks, error = closureExec.execute()
		if error != nil {
			return nil, error
		}
		return chunks, nil
	}
	e, error := store.svr.buildDAGExecutor(dagCtx, dagReq.Executors)
	if error != nil {
		return nil, error
	}
	ctx := context.TODO()
	for {
		var row [][]byte
		e.Counts()
		row, err = e.Next(ctx)
		if err != nil {
			break
		}
		if row == nil {
			break
		}
		data := dummySlice
		for _, offset := range dagReq.OutputOffsets {
			data = append(data, row[offset]...)
		}
		chunks = appendRow(chunks, data, rowCnt)
		rowCnt++
	}
	return chunks, nil
}

// see tikv/src/coprocessor/util.rs for more detail
func TestIsPrefixNext(t *testing.T) {
	require.True(t, isPrefixNext([]byte{}, []byte{0}))
	require.True(t, isPrefixNext([]byte{0}, []byte{1}))
	require.True(t, isPrefixNext([]byte{1}, []byte{2}))
	require.True(t, isPrefixNext([]byte{255}, []byte{255, 0}))
	require.True(t, isPrefixNext([]byte{255, 255, 255}, []byte{255, 255, 255, 0}))
	require.True(t, isPrefixNext([]byte{1, 255}, []byte{2, 0}))
	require.True(t, isPrefixNext([]byte{0, 1, 255}, []byte{0, 2, 0}))
	require.True(t, isPrefixNext([]byte{0, 1, 255, 5}, []byte{0, 1, 255, 6}))
	require.True(t, isPrefixNext([]byte{0, 1, 5, 255}, []byte{0, 1, 6, 0}))
	require.True(t, isPrefixNext([]byte{0, 1, 255, 255}, []byte{0, 2, 0, 0}))
	require.True(t, isPrefixNext([]byte{0, 255, 255, 255}, []byte{1, 0, 0, 0}))
}

func TestPointGet(t *testing.T) {
	var (
		data   *data
		store  *TestStore
		err    error
		errors []error
		chunks []tipb.Chunk
		row    []types.Datum
		l      int
	)
	// here would build mvccStore and server, and prepare
	// three rows data, just like the test data of table_scan.rs.
	// then init the store with the generated data.
	data = PrepareTestTableData(t, keyNumber, TableId)
	store, err = NewTestStore()
	require.Nil(t, err)
	errors = store.InitTestData(data.encodedKVDatas)
	require.Nil(t, errors)
	// point get should return nothing when handle is math.MinInt64
	chunks, err = executeAPointGet(data, store, math.MinInt64)
	require.Nil(t, err)
	require.Equal(t, len(chunks), 0)
	// point get should return one row when handler = 0
	var handle int64 = 0
	chunks, err = executeAPointGet(data, store, handle)
	require.Nil(t, err)
	require.Equal(t, 1, len(chunks))
	row, err = codec.Decode(chunks[0].RowsData, 2)
	require.Nil(t, err)
	require.Equal(t, len(row), 2)

	realDatum := data.rowValues[handle]
	l, err = row[0].CompareDatum(nil, &realDatum[0])
	require.Nil(t, err)
	require.Equal(t, l, 0)
	l, err = row[1].CompareDatum(nil, &realDatum[1])
	require.Nil(t, err)
	require.Equal(t, l, 0)
}
