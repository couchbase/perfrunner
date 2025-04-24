[clusters]
zeus =
    zeus-srv-01.perf.couchbase.com:kv
    zeus-srv-02.perf.couchbase.com:kv
    zeus-srv-03.perf.couchbase.com:kv
    zeus-srv-04.perf.couchbase.com:kv
    zeus-srv-05.perf.couchbase.com:n1ql
    zeus-srv-06.perf.couchbase.com:index

[clients]
hosts =
    zeus-cnt-01.perf.couchbase.com

[storage]
data = f:\data
index = e:\data

[metadata]
source = zeus

[parameters]
OS = Windows Server 2012
CPU = Data: E5-2630 v2 (24 vCPU), Query & Index: E5-2680 v3 (48 vCPU)
Memory = Data: 64GB, Query & Index: 256GB
Disk = Samsung Pro 850
