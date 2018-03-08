[clusters]
nyx =
    172.23.99.241:kv
    172.23.99.245:kv
    172.23.99.122:index
    172.23.99.246:kv
    172.23.99.251:kv
    172.23.99.118:kv
    172.23.99.119:kv
    172.23.99.123:kv
    172.23.99.247:kv
    172.23.98.118:kv
    172.23.98.119:kv

[clients]
hosts =
    172.23.99.87
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Data: E5-2630 (24 vCPU), Index: CPU E5-2680 v3 (48 vCPU)
Memory = Data: 64 GB, Index: 512 GB
Disk = Samsung PM863 SSD
