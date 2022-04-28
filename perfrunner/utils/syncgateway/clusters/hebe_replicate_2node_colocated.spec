[clusters]
hebe_c1 =
    172.23.100.190:kv,index,n1ql
hebe_c2 =
    172.23.100.204:kv,index,n1ql

[syncgateways]
syncgateways =
    172.23.100.191
    172.23.100.192
    172.23.100.193
    172.23.100.205
    172.23.100.206
    172.23.100.207

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
