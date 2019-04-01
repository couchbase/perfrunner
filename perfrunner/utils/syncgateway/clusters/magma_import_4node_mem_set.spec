[clusters]
magma =
    172.23.97.21:index
    172.23.97.22:index
    172.23.97.23:index
    172.23.97.29:index

[clients]
hosts =
    172.23.97.36
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
