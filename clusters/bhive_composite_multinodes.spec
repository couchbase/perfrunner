[clusters]
bhive_composite =
    172.23.100.85:kv
    172.23.100.86:kv
    172.23.100.88:kv
    172.23.100.87:index,n1ql
    172.23.100.81:index,n1ql
    172.23.100.83:index,n1ql
    172.23.100.84:index,n1ql
    172.23.100.82:index

[clients]
hosts =
    172.23.100.154
credentials = root:couchbase

[storage]
data = /data

[metadata]
cluster = bhive
