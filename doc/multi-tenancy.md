# multi-tenancy draft design

## Motivation

Multi-tenancy can greatly reduce the maintenance and hardware cost.

## Design

### Architecture Overview

- TiDB instance serves a single tenant.
- KV cluster serves multiple tenants.
- PD cluster serves multiple tenants.

### Key space isolation

Add a 4 bytes prefix to every key to store tenant ID.
The cluster ID can be used as the tenant ID when connects to PD.
During connection handshake, the client connection pass the tenant ID to KV node.
So each tenant creates its own connection.
We can obtain the tenant ID from the Context of each RPC call, only serve the data under the
tenant ID prefix.

### Resource Isolation

There is no resource isolation, all tenants shares the CPU, Memory, Disk, Network resources.
To ensure a tenant's SLA, we limit the tenant's service quota.

### Quota

quota limits the resource usage of a tenant, there are three type of quota: 
read_quota, write_quota, storage_quota.

the read_quota and write_quota is temporal, refreshed every minute.
storage_quota is persistent.

We deduct the quota after serving the tenant's request,
When tenant's quota deducted to zero, we stop serving the request and returns an error.

Every tenant has a total quota, PD is responsible to divide the total quota to each KV node that
stores the tenant's data.

#### Read Quota

The equation looks like:

```
read_quota = w1 * request_count + w2 * scan_cnt + w3 * response_size
```

w1, w2, w3 is the weight value for each operation.

#### Write Quota

The equation looks like:
```
write_quota = w1 * key_value_count + w2 * key_value_size
```

#### Storage Quota

The equation looks like:
```
storage_quota = w1 * key_value_count + w2 * key_value size.
```

### Security

- Communication layer security can be ensured by TLS.
- SST do not do any encryption, only do encrypt on backup.
