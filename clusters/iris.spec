[clusters]
iris =
    172.23.100.70
    172.23.100.71
    172.23.100.72
    172.23.100.73
    172.23.100.55,n1ql
    172.23.100.45,index

[clients]
hosts =
    172.23.100.44
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Data: E5-2630 v2 (24 vCPU), Query & Index: E5-2680 v3 (48 vCPU)
Memory = Data & Query: 64GB, Index: 512GB
Disk = SSD
