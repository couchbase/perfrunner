[clusters]
magma-nvme =
    172.23.100.135:kv
    172.23.100.136:kv
    172.23.100.137:kv
    172.23.100.80:index,n1ql
    172.23.100.81:index,n1ql
    172.23.100.82:index,n1ql
    172.23.100.83:index,n1ql
    172.23.100.84:index,n1ql

[clients]
hosts =
    172.23.100.154
credentials = root:couchbase

[storage]
data = /data

[metadata]
cluster = magma-nvme
