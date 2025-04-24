[clusters]
triton =
    triton-srv-01-ip6.perf.couchbase.com:kv
    triton-srv-02-ip6.perf.couchbase.com:kv
    triton-srv-03-ip6.perf.couchbase.com:kv
    triton-srv-04-ip6.perf.couchbase.com:kv
    triton-srv-05-ip6.perf.couchbase.com:index
    triton-srv-06-ip6.perf.couchbase.com:n1ql

[clients]
hosts =
    172.23.132.14
    172.23.132.13

[storage]
data = /data

[parameters]
OS = Ubuntu 20.04
CPU = Data: E5-2630 v4 (40 vCPU), Query & Index: E5-2680 v3 (48 vCPU)
Memory = Data & Query: 64GB, Index: 256GB
Disk = Samsung SM863
