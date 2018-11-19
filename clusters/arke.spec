[clusters]
arke =
    172.23.97.12:kv
    172.23.97.13:kv
    172.23.97.14:kv
    172.23.97.15:kv
    172.23.97.18:query
    172.23.97.19:index
    172.23.97.20:index

[clients]
hosts =
    172.23.97.16
    172.23.97.17
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2630 v2 (24 kv, 49 index vCPU)
Memory = 256 GB index, 64 GB query
Disk = Samsung SM863

