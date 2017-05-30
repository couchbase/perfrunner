[clusters]
ycsb3 =
    172.23.100.204
    172.23.100.205
    172.23.100.206

[clients]
hosts =
    172.23.100.194
    172.23.100.204
    172.23.100.205
    172.23.100.206
    172.23.100.207
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2680 v3 (48 vCPU)
Memory = 64GB
Disk = Samsung Pro 850
