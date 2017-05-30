[clusters]
triton_c1 =
    172.23.132.17
    172.23.132.18

triton_c2 =
    172.23.132.19
    172.23.132.20

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
Disk = Samsung SM863
