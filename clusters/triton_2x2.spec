[clusters]
triton_c1 =
    triton-srv-01.perf.couchbase.com:kv
    triton-srv-02.perf.couchbase.com:kv

triton_c2 =
    triton-srv-03.perf.couchbase.com:kv
    triton-srv-04.perf.couchbase.com:kv

[clients]
hosts =
    triton-cnt-01.perf.couchbase.com
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2630 v4 (40 vCPU)
Memory = Data: 64GB
Disk = Samsung SM863
