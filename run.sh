rm -f nohup.out
pkill  tidb-server
pkill  node 
#pkill pd-server

mkdir data

export host_ip="127.0.0.1"
unset http_proxy
unset HTTP_PROXY
unset HTTPS_PROXY
unset https_proxy

nohup $HOME/projects/src/github.com/pingcap/pd/bin/pd-server -client-urls="http://${host_ip}:2379" -peer-urls="http://${host_ip}:2380"   --data-dir=data/pd --log-file=pd.log & 
sleep 2 
nohup ./node/node -store-addr=${host_ip}:9191 -L=info -sync-write=false -table-loading-mode=memory-map -max-table-size=300663296 -num-level-zero-tables=30 -region-size=6655443 -num-mem-tables=5 -value-threshold=2560 -db-path=data -pd-addr="http://${host_ip}:2379" &
sleep 2 
nohup $HOME/projects/src/github.com/pingcap/tidb/bin/tidb-server -P 4000 -status 10081 --store=tikv --path="${host_ip}:2379?cluster=1" --log-file=tidb.log --config=./tidbconf & 
#nohup $HOME/projects/src/github.com/pingcap/tidb/bin/tidb-server -P 4000 -status 10081 --store=faketikv --path="$(pwd)/data" --log-file=tidb.log & 
