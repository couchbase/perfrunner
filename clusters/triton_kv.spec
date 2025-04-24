[clusters]
triton =
    triton-srv-01-ip6.perf.couchbase.com:kv
    triton-srv-02-ip6.perf.couchbase.com:kv
    triton-srv-03-ip6.perf.couchbase.com:kv
    triton-srv-04-ip6.perf.couchbase.com:kv

[clients]
hosts =
    triton-cnt-01.perf.couchbase.com

[storage]
data = /data
backup = /workspace/backup

[parameters]
OS = Ubuntu 20.04
CPU = Data: E5-2630 v4 (40 vCPU)
Memory = 64GB
Disk = Samsung SM863
