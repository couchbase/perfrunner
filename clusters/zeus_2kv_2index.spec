[clusters]
zeus =
    zeus-srv-01.perf.couchbase.com:kv
    zeus-srv-02.perf.couchbase.com:kv
    zeus-srv-03.perf.couchbase.com:index
    zeus-srv-04.perf.couchbase.com:index

[clients]
hosts =
    zeus-cnt-01.perf.couchbase.com
credentials = root:couchbase

[storage]
data = f:\data
index = e:\data

[credentials]
rest = Administrator:password
ssh = Administrator:Membase123

[parameters]
OS = Windows Server 2012
CPU = Data: E5-2630 v2 (24 vCPU), Query & Index: E5-2680 v3 (48 vCPU)
Memory = Data: 64GB, Query & Index: 256GB
Disk = Samsung Pro 850
