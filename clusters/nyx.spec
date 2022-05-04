[clusters]
nyx =
    172.23.97.3:kv
    172.23.97.4:kv
    172.23.97.10:index
    172.23.97.5:kv
    172.23.97.6:kv
    172.23.97.7:kv

[clients]
hosts =
    172.23.97.9
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Data: E5-2630 (24 vCPU), Index: CPU E5-2680 v3 (48 vCPU)
Memory = Data: 64 GB, Index: 512 GB
Disk = Samsung PM863 SSD
