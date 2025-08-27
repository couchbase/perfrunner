[clusters]
hemera =
    cen-s723.perf.couchbase.com:kv
    cen-s712.perf.couchbase.com:kv
    cen-s705.perf.couchbase.com:index
    cen-s710.perf.couchbase.com:kv
    cen-s709.perf.couchbase.com:kv
    cen-s708.perf.couchbase.com:kv

[clients]
hosts =
    ubu-s703.perf.couchbase.com

[storage]
data = /data
index = /nvme

[metadata]
source = hemera
cluster = hemera
