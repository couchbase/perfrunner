[clusters]
poseidon =
    172.23.132.10:8091
    172.23.132.11:8091
    172.23.132.12:8091
    172.23.132.13:8091
    172.23.132.15:8091,index

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
CPU = Data: E5-2630 v2 (24 vCPU), Index: E5-2680 v3 (48 vCPU)
Memory = Data: 64GB, Index: 256GB
Disk = SSD
