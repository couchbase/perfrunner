[clusters]
hwcluster =
    172.23.98.29:kv
    172.23.98.30:cbas
    172.23.98.31:kv

[clients]
hosts =
    172.23.98.32
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase
