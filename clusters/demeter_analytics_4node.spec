[clusters]
demeter =
    172.23.100.161:kv
    172.23.100.162:kv
    172.23.100.163:cbas
    172.23.100.9:cbas

[clients]
hosts =
    172.23.100.165
credentials = root:couchbase

[storage]
data = /data
analytics = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2630 v3 (32 vCPU)
Memory = 64 GB
Disk = Samsung 860 1TB
