package raftstore

import (
	"fmt"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

type testSplitCheckFsm struct {
	stopped bool
	recv    <-chan Msg
	mailbox *mailbox
}

func (s *testSplitCheckFsm) isStopped() bool {
	return s.stopped
}

func (s *testSplitCheckFsm) setMailbox(mb *mailbox) {
	s.mailbox = mb
}

func (s *testSplitCheckFsm) takeMailbox() *mailbox {
	mb := s.mailbox
	s.mailbox = nil
	return mb
}

func newSplitCheckFsm(cap int) (chan Msg, fsm) {
	ch := make(chan Msg, cap)
	r := &testSplitCheckFsm{
		recv: ch,
	}
	return ch, r
}

func newTestRouter() (*router, chan Msg) {

	ctrlSender, ctrlFsm := newSplitCheckFsm(10)
	normarlSender, normalFsm := newSplitCheckFsm(10)
	ctrlBox := newMailbox(ctrlSender, ctrlFsm)

	normalScheCh := make(chan Msg, msgDefaultChanSize)
	normalSche := &normalScheduler{sender: normalScheCh}

	router := newRouter(ctrlBox, normalSche, normalSche)
	normalBox := newMailbox(normarlSender, normalFsm)
	router.register(1, normalBox)
	return router, normarlSender
}

func newTestSplitCheckRegion() *metapb.Region {
	region := &metapb.Region{
		Id: 1,
		Peers: []*metapb.Peer{
			new(metapb.Peer),
		},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 5,
			Version: 2,
		},
		StartKey: codec.EncodeInt(tablecodec.TablePrefix(), int64(0)),
		EndKey:   codec.EncodeInt(tablecodec.TablePrefix(), int64(0)),
	}
	return region
}

func runTestSplitCheckTask(runner *splitCheckRunner, autoSplit bool, region *metapb.Region,
	policy pdpb.CheckPolicy) {
	runner.run(task{
		tp: taskTypeSplitCheck,
		data: &splitCheckTask{
			region:    region,
			autoSplit: autoSplit,
			policy:    policy,
		},
	})
}

func mustSplitAt(t *testing.T, rx <-chan Msg, expectRegion *metapb.Region, expectSplitKeys [][]byte) {
	msg := <-rx
	if msg.Type == MsgTypeRegionApproximateSize || msg.Type == MsgTypeRegionApproximateKeys {
		assert.Equal(t, msg.RegionID, expectRegion.Id)
	} else if msg.Type == MsgTypeSplitRegion {
		assert.Equal(t, msg.RegionID, expectRegion.Id)
		splitRegion := msg.Data.(*MsgSplitRegion)
		assert.Equal(t, splitRegion.RegionEpoch, expectRegion.RegionEpoch)
		assert.Equal(t, splitRegion.SplitKeys, expectSplitKeys)
	} else {
		panic(fmt.Sprintf("expect split check result. but got %v", msg))
	}
}

func putData(db *DBBundle, startIdx, endIdx uint64) {
	val := []byte("some value")
	arr := [1024]byte{}
	for startIdx < endIdx {
		batchIdx := startIdx + 20
		if batchIdx > endIdx {
			batchIdx = endIdx
		}
		writeBatch := new(WriteBatch)
		for i := startIdx; i < batchIdx; i++ {
			key := DataKey(codec.EncodeBytes(nil, []byte(fmt.Sprintf("%04d", i))))
			writeBatch.Set(key, val)
			writeBatch.Set(key, arr[:])
		}
		// Flush to generate SST files, so that properties can be utilized.
		writeBatch.WriteToKV(db)
		startIdx = batchIdx
	}
}

func TestKeysSplitCheck(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)

	config := newDefaultSplitCheckConfig()
	config.regionMaxKeys = 100
	config.regionSplitKeys = 90
	config.batchSplitLimit = 5

	region := newTestSplitCheckRegion()

	router, receiver := newTestRouter()
	host := newCoprocessorHost(config, router)
	runner := newSplitCheckRunner(engines.kv.db, router, host)

	// so split key will be z0080
	putData(engines.kv, 0, 90)
	runTestSplitCheckTask(runner, true, region, pdpb.CheckPolicy_SCAN)

	// keys has not reached the maxKeys 100 yet.
	msg := <-receiver
	if msg.Type != MsgTypeRegionApproximateKeys && msg.Type != MsgTypeRegionApproximateSize {
		panic(fmt.Sprintf("expect recv empty, but got %v", msg))
	}
	assert.Equal(t, msg.RegionID, region.GetId())

	putData(engines.kv, 90, 160)
	runTestSplitCheckTask(runner, true, region, pdpb.CheckPolicy_SCAN)
	mustSplitAt(t, receiver, region, [][]byte{
		[]byte("0080"),
	})

	putData(engines.kv, 160, 300)
	runTestSplitCheckTask(runner, true, region, pdpb.CheckPolicy_SCAN)
	mustSplitAt(t, receiver, region, [][]byte{
		[]byte("0080"), []byte("0160"),
		[]byte("0240"),
	})

	putData(engines.kv, 300, 500)
	runTestSplitCheckTask(runner, true, region, pdpb.CheckPolicy_SCAN)
	mustSplitAt(t, receiver, region, [][]byte{
		[]byte("0080"), []byte("0160"),
		[]byte("0240"), []byte("0320"),
		[]byte("0400"),
	})
}

func TestHalfSplitCheck(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)

	region := newTestSplitCheckRegion()

	config := newDefaultSplitCheckConfig()
	config.regionMaxSize = BucketNumberLimit

	router, receiver := newTestRouter()
	host := newCoprocessorHost(config, router)
	runner := newSplitCheckRunner(engines.kv.db, router, host)

	// so split key will be z0005
	putData(engines.kv, 0, 11)
	runTestSplitCheckTask(runner, false, region, pdpb.CheckPolicy_SCAN)
	splitKeys := []byte("0005")
	mustSplitAt(t, receiver, region, [][]byte{splitKeys})
	runTestSplitCheckTask(runner, false, region, pdpb.CheckPolicy_APPROXIMATE)
	mustSplitAt(t, receiver, region, [][]byte{splitKeys})
}

func TestSizeSplitCheck(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)

	region := newTestSplitCheckRegion()

	config := newDefaultSplitCheckConfig()
	config.regionMaxSize = 100
	config.regionSplitSize = 60
	config.batchSplitLimit = 5

	router, receiver := newTestRouter()
	host := newCoprocessorHost(config, router)
	runner := newSplitCheckRunner(engines.kv.db, router, host)

	// so split key will be [z0006]
	putData(engines.kv, 0, 7)
	runTestSplitCheckTask(runner, true, region, pdpb.CheckPolicy_SCAN)
	// size has not reached the max_size 100 yet.
	msg := <-receiver
	if msg.Type != MsgTypeRegionApproximateKeys && msg.Type != MsgTypeRegionApproximateSize {
		panic(fmt.Sprintf("expect recv empty, but got %v", msg))
	}
	assert.Equal(t, msg.RegionID, region.GetId())

	putData(engines.kv, 7, 11)
	runTestSplitCheckTask(runner, true, region, pdpb.CheckPolicy_SCAN)
	mustSplitAt(t, receiver, region, [][]byte{[]byte("0006")})

	// so split keys will be [z0006, z0012]
	putData(engines.kv, 11, 19)
	runTestSplitCheckTask(runner, true, region, pdpb.CheckPolicy_SCAN)
	mustSplitAt(t, receiver, region, [][]byte{[]byte("0006"), []byte("0012")})

	// for test batch_split_limit
	// so split kets will be [z0006, z0012, z0018, z0024, z0030]
	putData(engines.kv, 19, 50)
	runTestSplitCheckTask(runner, true, region, pdpb.CheckPolicy_SCAN)
	mustSplitAt(t, receiver, region, [][]byte{
		[]byte("0006"), []byte("0012"),
		[]byte("0018"), []byte("0024"),
		[]byte("0030"),
	})
}

func TestCheckerWithSameMaxAndSplitSize(t *testing.T) {
	checker := newSizeSplitChecker(24, 24, 1, pdpb.CheckPolicy_SCAN)
	region := new(metapb.Region)
	ctx := observerContext{region: region}
	for {
		data := splitCheckKeyEntry{key: []byte("zxxxx"), valueSize: 4}
		if checker.onKv(&ctx, data) {
			break
		}
	}
	assert.True(t, len(checker.getSplitKeys()) != 0)
}

func TestCheckerWithMaxTwiceBiggerThanSplitSize(t *testing.T) {
	checker := newSizeSplitChecker(20, 10, 1, pdpb.CheckPolicy_SCAN)
	region := new(metapb.Region)
	ctx := observerContext{region: region}
	for i := 0; i < 2; i++ {
		data := splitCheckKeyEntry{key: []byte("zxxxx"), valueSize: 5}
		if checker.onKv(&ctx, data) {
			break
		}
	}
	assert.True(t, len(checker.getSplitKeys()) != 0)
}

func TestLastKeyOfRegion(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)

	region := &metapb.Region{
		Id: 1,
		Peers: []*metapb.Peer{
			new(metapb.Peer),
		},
	}

	writeBatch := new(WriteBatch)
	dataKeys := [2][]byte{}
	padding := []byte("_r00000005")
	for i := 1; i < 3; i++ {
		tableKey := codec.EncodeInt(tablecodec.TablePrefix(), int64(i))
		key := append(tableKey, padding...)
		key = DataKey(key)
		writeBatch.Set(key, key)
		dataKeys[i-1] = key
	}
	writeBatch.WriteToKV(engines.kv)

	checkCases := func(startId, endId int64, want []byte) {
		startTableKey := codec.EncodeInt(tablecodec.TablePrefix(), startId)
		endTableKey := codec.EncodeInt(tablecodec.TablePrefix(), endId)
		region.StartKey = startTableKey
		region.EndKey = endTableKey
		assert.Equal(t, lastKeyOfRegion(engines.kv.db, region), want)
	}

	// ["", "") => t2_xx
	startId := int64(0)
	endId := int64(math.MaxInt64)
	checkCases(startId, endId, dataKeys[1])
	// ["t0", "t1") => None
	endId = 1
	checkCases(startId, endId, nil)
	startId = int64(1)
	endId = int64(math.MaxInt64)
	// ["t1", "tMax") => t2_xx
	checkCases(startId, endId, dataKeys[1])
	// ["t1", "t2") => t1_xx
	endId = 2
	checkCases(startId, endId, dataKeys[0])
}

func TestTableSplitCheckObserver(t *testing.T) {
	engines := newTestEngines(t)
	defer cleanUpTestEngineData(engines)

	region := newTestSplitCheckRegion()

	config := newDefaultSplitCheckConfig()
	// Enable table split.
	config.splitRegionOnTable = true
	// Try to "disable" size split.
	config.regionMaxSize = uint64(2 * 1024 * 1024 * 1024)
	config.regionSplitSize = uint64(1024 * 1024 * 1024)
	// Try to "disable" keys split
	config.regionMaxKeys = uint64(2000000000)
	config.regionSplitKeys = uint64(1000000000)

	// only create table split observer and ignore approximate.
	router, receiver := newTestRouter()
	host := &CoprocessorHost{}
	host.registry.splitCheckObservers = append(host.registry.splitCheckObservers, &tableSplitCheckObserver{})
	runner := newSplitCheckRunner(engines.kv.db, router, host)

	generateTablePrefix := func(tableId int64) []byte {
		return codec.EncodeInt(tablecodec.TablePrefix(), tableId)
	}

	checkCases := func(encodedStartKey, encodedEndKey []byte, tableId interface{}) {
		region.StartKey = encodedStartKey
		region.EndKey = encodedEndKey
		runTestSplitCheckTask(runner, true, region, pdpb.CheckPolicy_SCAN)
		switch tableId.(type) {
		case nil:
			assert.Equal(t, 0, len(receiver))
		case uint64:
			key := generateTablePrefix(int64(tableId.(uint64)))
			msg := <-receiver
			if msg.Type == MsgTypeSplitRegion {
				splitRegion := msg.Data.(*MsgSplitRegion)
				assert.Equal(t, [][]byte{key}, splitRegion.SplitKeys)
			} else {
				panic(fmt.Sprintf("expect %v but got %v", key, msg.Type))
			}
		}

	}

	// arbitrary padding.
	padding := []byte("_r00000005")

	// Put some tables
	// t1_xx, t3_xx
	wb := new(WriteBatch)
	for i := 1; i < 4; i++ {
		if i%2 == 0 {
			// leave some space.
			continue
		}
		key := append(generateTablePrefix(int64(i)), padding...)
		key = DataKey(key)
		wb.Set(key, key)
	}
	wb.WriteToKV(engines.kv)

	// ["t0", "t2") => t1
	checkCases(generateTablePrefix(int64(0)), generateTablePrefix(int64(2)), uint64(1))
	// ["t1", "tMax") => t3
	checkCases(generateTablePrefix(int64(1)), generateTablePrefix(math.MaxInt64), uint64(3))
	// ["t1", "t5") => t3
	checkCases(generateTablePrefix(int64(1)), generateTablePrefix(int64(5)), uint64(3))
	// ["t2", "t4") => t3
	checkCases(generateTablePrefix(int64(2)), generateTablePrefix(int64(4)), uint64(3))

	// Put some data to t3
	wb = new(WriteBatch)
	for i := 1; i < 4; i++ {
		key := append(generateTablePrefix(int64(3)), padding...)
		key = append(key, byte(i))
		key = DataKey(key)
		wb.Set(key, key)
	}
	wb.WriteToKV(engines.kv)

	// ["t1", "tMax") => t3
	checkCases(generateTablePrefix(int64(1)), generateTablePrefix(math.MaxInt64), uint64(3))
	// ["t3", "tMax") => skip
	checkCases(generateTablePrefix(int64(3)), generateTablePrefix(math.MaxInt64), nil)
	// ["t3", "t5") => skip
	checkCases(generateTablePrefix(int64(3)), generateTablePrefix(int64(5)), nil)
}
