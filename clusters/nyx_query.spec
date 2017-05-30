[clusters]
nyx =
    172.23.99.241
    172.23.99.245
    172.23.99.122,index
    172.23.99.246
    172.23.99.251
    172.23.99.118
    172.23.99.119
    172.23.99.123
    172.23.99.247,n1ql
    172.23.98.118
    172.23.98.119

[clients]
hosts =
    172.23.99.87
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Data & Query: E5-2630 (24 vCPU), Index: CPU E5-2680 v3 (48 vCPU)
Memory = Data: 64 GB, Index: 512 GB
Disk = Samsung Pro 850
