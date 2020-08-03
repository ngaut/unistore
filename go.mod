module github.com/ngaut/unistore

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.4
	github.com/google/btree v1.0.0
	github.com/juju/errors v0.0.0-20181118221551-089d3ea4e4d5
	github.com/pierrec/lz4 v2.0.5+incompatible
	github.com/pingcap/badger v1.5.1-0.20200714132513-80ba2000f159
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20200729012136-4e113ddee29e
	github.com/pingcap/kvproto v0.0.0-20200803054707-ebd5de15093f
	github.com/pingcap/log v0.0.0-20200511115504-543df19646ad
	github.com/pingcap/pd/v4 v4.0.0-rc.2.0.20200730093003-dc8c75cf7ca0
	github.com/pingcap/tidb v1.1.0-beta.0.20200803035726-41c23700d8d1
	github.com/prometheus/client_golang v1.5.1
	github.com/shirou/gopsutil v2.19.10+incompatible
	github.com/stretchr/testify v1.5.1
	github.com/uber-go/atomic v1.3.2
	github.com/zhangjinpeng1987/raft v0.0.0-20190624145930-deeb32d6553d
	go.uber.org/zap v1.15.0
	golang.org/x/net v0.0.0-20200520182314-0ba52f642ac2
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/grpc v1.26.0
)

replace go.etcd.io/etcd => github.com/zhangjinpeng1987/etcd v0.0.0-20190226085253-137eac022b64

go 1.13
