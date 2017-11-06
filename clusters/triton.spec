[clusters]
triton =
    triton-srv-01.perf.couchbase.com:kv
    triton-srv-02.perf.couchbase.com:kv
    triton-srv-03.perf.couchbase.com:kv
    triton-srv-04.perf.couchbase.com:kv
    triton-srv-05.perf.couchbase.com:index
    triton-srv-06.perf.couchbase.com:n1ql

[clients]
hosts =
    172.23.132.14
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Data: E5-2630 v4 (40 vCPU), Query & Index: E5-2680 v3 (48 vCPU)
Memory = Data & Query: 64GB, Index: 256GB
Disk = Samsung SM863
