# unistore
A fun project for evaluating some new optimizations quickly, do not use it in production.

## Build
```sh
make linux
make compaction-linux
make s3tool-linux
```

## Deploy using TiUP

Put the binary of `bin/unistore-server-linux`, `bin/compaction-server-linux` and `bin/s3tool-linux` into a single directory.

#### Step 1: Use MinIO to simulate S3

Use the following command to run a standalone MinIO server. Replace `/data` with the path to the drive or directory in which you want MinIO to store data.
```sh
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
./minio server --address 10.0.1.5:9000 /data
```
Create the `shard-db` bucket.
```sh
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
./mc alias set minio http://10.0.1.5:9000 minioadmin minioadmin
./mc mb minio/shard-db
```

#### Step 2: Start compaction server

```sh
./compaction-server-linux --addr "10.0.1.6:9080"
```

#### Step 3: Install TiUP on the control machine

```sh
curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh
source ~/.bash_profile
```

#### Step 4: Initialize cluster topology file

Execute `vi topology.yaml` to write the configuration file content.
See [deploy/topology.yaml](https://github.com/ngaut/unistore/blob/sharding/deploy/topology.yaml 'topology.yaml') configuration.

#### Step 5: Execute the deployment command

Before you execute the `deploy` command, use the `check` and `check --apply` commands to detect and automatically repair the potential risks in the cluster:
```sh
tiup cluster check ./topology.yaml --user tidb [-p] [-i /home/tidb/.ssh/gcp_rsa]
tiup cluster check ./topology.yaml --apply --user tidb [-p] [-i /home/tidb/.ssh/gcp_rsa]
```

Then execute the `deploy` command to deploy the TiDB cluster:
```sh
tiup cluster deploy tidb-test v5.0.1 ./topology.yaml --ignore-config-check   \
    --user tidb [-p] [-i /home/tidb/.ssh/gcp_rsa]
```

#### Step 6: Check the clusters managed by TiUP

```sh
tiup cluster list
```

#### Step 7: Replace the TiKV binaries

Prepare a TiKV hotfix.
```sh
mv unistore-server-linux  tikv-server
tar czf tikv-hotfix-linux-amd64.tar.gz tikv-server
```

Then execute the `patch` command to patch the TiDB cluster:
```sh
tiup cluster patch tidb-test tikv-hotfix-linux-amd64.tar.gz --role tikv --offline
```

#### Step 8: Check the status of the deployed TiDB cluster

For example, execute the following command to check the status of the `tidb-test` cluster:
```sh
tiup cluster display tidb-test
```

#### Step 9: Start the TiDB cluster

```sh
tiup cluster start tidb-test
```

## Clean

Clean up the TiDB cluster.
```sh
tiup cluster clean tidb-test --all
./s3tool-linux -endpoint 10.0.1.5:9000 -key-id minioadmin -secret-key minioadmin  \
    -bucket shard-db -region local -instance-id 1 clean
```

## Destroy

Destroy the TiDB cluster.
```sh
tiup cluster destroy tidb-test
```