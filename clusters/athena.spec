[clusters]
athena =
    cen-sc34.perf.couchbase.com:kv
    cen-sc33.perf.couchbase.com:kv
    cen-sc32.perf.couchbase.com:kv
    cen-sc31.perf.couchbase.com:kv

[clients]
hosts =
    ubu-sc30.perf.couchbase.com
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = 2 x Gold 6230 (80 vCPU)
Memory = 128 GB
Disk = Samsung SM863