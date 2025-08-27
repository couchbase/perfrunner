[clusters]
aether =
    172.23.110.53:kv
    172.23.110.54:kv
    172.23.110.56:kv
    172.23.110.71:index,n1ql
    172.23.110.72:index,n1ql
    172.23.110.55:index,n1ql
    172.23.110.73:kv

[clients]
hosts =
    172.23.110.74

[storage]
data = /data

[metadata]
cluster = aether
