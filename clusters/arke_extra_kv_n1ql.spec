[clusters]
arke =
    172.23.97.12:kv
    172.23.97.13:kv
    172.23.97.14:kv
    172.23.97.15:kv
    172.23.97.18:n1ql
    172.23.97.19:index
    172.23.97.20:kv,n1ql

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
OS = Ubuntu 20.04
CPU = Data: E5-2630 v2 (24 vCPU), Query & Index: E5-2680 v3 (48 vCPU)
Memory = Data & Query: 64GB, Index: 320GB
Disk = Samsung SM863
