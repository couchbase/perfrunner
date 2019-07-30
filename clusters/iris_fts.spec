[clusters]
iris =
    172.23.100.70:kv
    172.23.100.71:kv
    172.23.100.72:kv
    172.23.100.45:fts
    172.23.100.55:n1ql

[clients]
hosts =
    172.23.100.44
    172.23.100.104
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Data: E5-2630 v2 (24 vCPU), Query & Index: E5-2680 v3 (48 vCPU)
Memory = Data & Query: 64GB, Index: 512GB, Search: 20GB
Disk = Samsung Pro 850
