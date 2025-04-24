[clusters]
triton_c1 =
    triton-srv-01-ip6.perf.couchbase.com:kv
    triton-srv-02-ip6.perf.couchbase.com:kv

triton_c2 =
    triton-srv-03-ip6.perf.couchbase.com:kv
    triton-srv-04-ip6.perf.couchbase.com:kv

[clients]
hosts =
    triton-cnt-01.perf.couchbase.com

[storage]
data = /data

[parameters]
OS = Ubuntu 20.04
CPU = E5-2630 v4 (40 vCPU)
Memory = Data: 64GB
Disk = Samsung SM863
