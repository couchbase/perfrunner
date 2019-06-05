[clusters]
hebe =
    172.23.100.190:kv
    172.23.100.191:kv
    172.23.100.192:kv
    172.23.100.193:n1ql
    172.23.100.207:index

[clients]
hosts =
    172.23.100.194
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
