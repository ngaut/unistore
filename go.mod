module github.com/ngaut/unistore

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/arl/statsviz v0.4.0
	github.com/aws/aws-sdk-go-v2 v1.6.0
	github.com/aws/aws-sdk-go-v2/config v1.3.0
	github.com/aws/aws-sdk-go-v2/credentials v1.2.1
	github.com/aws/aws-sdk-go-v2/service/s3 v1.9.0
	github.com/cespare/xxhash v1.1.0
	github.com/coocood/bbloom v0.0.0-20190830030839-58deb6228d64
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.3.4
	github.com/google/btree v1.0.0
	github.com/ncw/directio v1.0.4
	github.com/pingcap/badger v1.5.1-0.20210331054718-0885bbfa8520
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/kvproto v0.0.0-20210308063835-39b884695fb8
	github.com/pingcap/log v0.0.0-20210317133921-96f4fcab92a4
	github.com/pingcap/parser v0.0.0-20210330190622-f959a136fc19
	github.com/pingcap/tidb v1.1.0-beta.0.20210407104700-3d8084e972d1
	github.com/pingcap/tipb v0.0.0-20210326161441-1164ca065d1b
	github.com/prometheus/client_golang v1.5.1
	github.com/shirou/gopsutil v3.21.2+incompatible
	github.com/soheilhy/cmux v0.1.5
	github.com/stretchr/testify v1.6.1
	github.com/tikv/pd v1.1.0-beta.0.20210323121136-78679e5e209d
	github.com/twmb/murmur3 v1.1.3
	github.com/uber-go/atomic v1.4.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20210316092652-d523dce5a7f4
	golang.org/x/sys v0.0.0-20210324051608-47abb6519492
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/grpc v1.27.1
)

replace go.etcd.io/etcd => github.com/zhangjinpeng1987/etcd v0.5.0-alpha.5.0.20201117041249-9487a87e2cbd

replace github.com/pingcap/kvproto => github.com/coocood/kvproto v0.0.0-20210712100210-4afaae04ba8e

go 1.13
