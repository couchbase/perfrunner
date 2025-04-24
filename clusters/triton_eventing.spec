[clusters]
triton =
    triton-srv-01-ip6.perf.couchbase.com:kv
    triton-srv-02-ip6.perf.couchbase.com:kv
    triton-srv-03-ip6.perf.couchbase.com:kv
    triton-srv-04-ip6.perf.couchbase.com:kv
    triton-srv-05-ip6.perf.couchbase.com:eventing

[clients]
hosts =
    triton-cnt-01.perf.couchbase.com

[storage]
data = /data

[parameters]
OS = Ubuntu 20.04
CPU = Data: E5-2630 v4 (40 vCPU), Eventing: E5-2680 v3 (48 vCPU)
Memory = Data: 64GB, Eventing: 256GB
Disk = Samsung SM863
