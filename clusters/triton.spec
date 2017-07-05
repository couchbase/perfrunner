[clusters]
triton =
    172.23.132.17:kv
    172.23.132.18:kv
    172.23.132.19:kv
    172.23.132.20:kv
    172.23.132.15:index
    172.23.132.16:n1ql

[clients]
hosts =
    172.23.132.14
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Data: E5-2630 v4 (40 vCPU), Query & Index: E5-2680 v3 (48 vCPU)
Memory = Data & Query: 64GB, Index: 256GB
Disk = Samsung SM863
