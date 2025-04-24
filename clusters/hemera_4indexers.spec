[clusters]
hemera =
    cen-s723.perf.couchbase.com:kv
    cen-s712.perf.couchbase.com:kv
    cen-s705.perf.couchbase.com:index
    cen-s710.perf.couchbase.com:index
    cen-s709.perf.couchbase.com:index
    cen-s708.perf.couchbase.com:index

[clients]
hosts =
    ubu-s703.perf.couchbase.com

[storage]
data = /data

[metadata]
source = hemera

[parameters]
OS = Ubuntu 20.04
CPU = Data: 2xGold 6230 (80 vCPU), Index: CPU 2xGold 6230 (80 vCPU)
Memory = Data: 128 GB, Index: 512 GB
Disk = Samsung DCT883 960GB
