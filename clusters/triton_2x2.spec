[clusters]
triton_c1 =
    172.23.132.19:8091
    172.23.132.20:8091

triton_c2 =
    172.23.132.19:8091
    172.23.132.20:8091

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
CPU = E5-2630 v4 (40 vCPU)
Memory = Data: 64GB
Disk = SSD
