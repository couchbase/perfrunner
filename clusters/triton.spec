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

[metadata]
cluster = triton
