[clusters]
arke_basic =
    172.23.97.12:kv
    172.23.97.13:kv
    172.23.97.14:kv
    172.23.97.19:index,n1ql
    172.23.97.20:index,n1ql
    172.23.97.177:eventing
    172.23.96.23:cbas
    172.23.96.20:fts
    172.23.97.15:kv
themis =
    172.23.96.16:kv
    172.23.96.17:kv

[clients]
hosts =
    172.23.97.16
    172.23.97.17
credentials = root:couchbase

[storage]
data = /data
backup = /workspace/backup

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = Ubuntu 20.04
CPU = Data: 2 x E5-2630 v2(24 vCPU)
Memory = Data: 64 GB
Disk = Samsung PM863 SSD
