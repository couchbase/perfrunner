[clusters]
magma =
    172.23.97.24:kv
    172.23.97.25:kv
    172.23.97.26:kv
    172.23.97.27:kv
    172.23.97.28:kv
    172.23.97.29:n1ql
    172.23.97.30:index

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
CPU = E5-2680 v4 (16 vCPU)
Memory = 32GB
Disk = Samsung DCT883
