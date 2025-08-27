[clusters]
leto =
    leto-srv-01.perf.couchbase.com:kv
    leto-srv-02.perf.couchbase.com:kv
    leto-srv-03.perf.couchbase.com:kv
    leto-srv-04.perf.couchbase.com:kv

[clients]
hosts =
    leto-cnt-01.perf.couchbase.com

[storage]
data = /data
index = /index
backup = /data/workspace/backup

[metadata]
cluster = leto
