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
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = Ubuntu 20.04
CPU = Data: 2xGold 6230 (80 vCPU), Index: CPU 2xGold 6230 (80 vCPU)
Memory = Data: 128 GB, Index: 512 GB
Disk = Samsung SSD 860 1TB
