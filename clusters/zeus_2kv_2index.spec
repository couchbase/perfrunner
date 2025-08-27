[clusters]
zeus =
    zeus-srv-01.perf.couchbase.com:kv
    zeus-srv-02.perf.couchbase.com:kv
    zeus-srv-03.perf.couchbase.com:index
    zeus-srv-04.perf.couchbase.com:index

[clients]
hosts =
    zeus-cnt-01.perf.couchbase.com

[storage]
data = f:\data
index = e:\data

[metadata]
source = zeus
cluster = zeus