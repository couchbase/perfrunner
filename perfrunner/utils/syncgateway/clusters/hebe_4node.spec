[clusters]
hebe =
    172.23.100.190:kv
    172.23.100.191:kv
    172.23.100.192:kv
    172.23.100.193:kv

[syncgateways]
syncgateways =
    172.23.100.204
    172.23.100.205
    172.23.100.206
    172.23.100.207

[clients]
hosts =
    172.23.97.250
    172.23.97.251
    172.23.97.252
    172.23.97.253
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
