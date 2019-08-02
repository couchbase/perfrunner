[clusters]
oceanus =
    triton-srv-01-ip6.perf.couchbase.com:kv
    triton-srv-02-ip6.perf.couchbase.com:kv
    triton-srv-03-ip6.perf.couchbase.com:cbas
    triton-srv-04-ip6.perf.couchbase.com:cbas
    triton-srv-05-ip6.perf.couchbase.com:cbas
    triton-srv-06-ip6.perf.couchbase.com:cbas

[clients]
hosts =
    172.23.132.14
credentials = root:couchbase

[storage]
data = /data
analytics = /data1 /data2

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2680 v3 (24 cores)
Memory = 32 GB
Disk = 2 x Samsung SM863
