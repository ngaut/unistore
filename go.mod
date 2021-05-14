module github.com/ngaut/unistore

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/arl/statsviz v0.4.0
	github.com/aws/aws-sdk-go v1.34.22
	github.com/cespare/xxhash v1.1.0
	github.com/coocood/bbloom v0.0.0-20190830030839-58deb6228d64
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.4
	github.com/google/btree v1.0.0
	github.com/ncw/directio v1.0.4
	github.com/pingcap/badger v1.5.1-0.20210331054718-0885bbfa8520
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20201021055732-210aacd3fd99
	github.com/pingcap/kvproto v0.0.0-20201023092649-e6d6090277c9
	github.com/pingcap/log v0.0.0-20200828042413-fce0951f1463
	github.com/pingcap/tidb v1.1.0-beta.0.20200820085534-0d997f2b8b3c
	github.com/prometheus/client_golang v1.5.1
	github.com/shirou/gopsutil v2.20.3+incompatible
	github.com/stretchr/testify v1.5.1
	github.com/tikv/pd v1.1.0-beta.0.20200910042021-254d1345be09
	github.com/uber-go/atomic v1.4.0
	github.com/zhangjinpeng1987/raft v0.0.0-20200819064223-df31bb68a018
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20200904194848-62affa334b73
	golang.org/x/sys v0.0.0-20200819171115-d785dc25833f
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/grpc v1.26.0
)

replace go.etcd.io/etcd => github.com/zhangjinpeng1987/etcd v0.0.0-20190226085253-137eac022b64

replace github.com/pingcap/tidb => github.com/coocood/tidb v0.0.0-20201117045115-1cfbea7d9a12

replace github.com/zhangjinpeng1987/raft => github.com/hslam/raft-1 v0.0.0-20210430053542-5b276eb6ffcd

go 1.13
