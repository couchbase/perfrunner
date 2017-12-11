[clusters]
oceanus =
    172.23.96.8:kv
    172.23.96.9:kv
    172.23.96.5:cbas
    172.23.96.7:cbas

[clients]
hosts =
    172.23.96.22
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
Memory = 64 GB
Disk = HDD ST1000NX0443/ST91000640NS
