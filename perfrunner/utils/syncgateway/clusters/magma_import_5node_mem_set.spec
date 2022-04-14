[clusters]
magma =
    172.23.97.24:index
    172.23.97.25:index
    172.23.97.26:index
    172.23.97.27:index
    172.23.97.28:index

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
