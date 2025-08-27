[clusters]
magma-nvme =
    172.23.100.135:kv
    172.23.100.136:kv
    172.23.100.137:kv
    172.23.100.80:index,n1ql

[clients]
hosts =
    172.23.100.139
credentials = root:couchbase

[storage]
data = /data

[metadata]
cluster = magma-nvme
