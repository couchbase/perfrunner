[clusters]
nyx =
    172.23.97.3:kv
    172.23.97.4:kv
    172.23.97.10:kv,index,n1ql
    172.23.97.5:kv
    172.23.97.6:kv
    172.23.97.7:kv

[clients]
hosts =
    172.23.97.9

[storage]
data = /data

[parameters]
OS = Ubuntu 20.04
CPU = Data: E5-2630 (24 vCPU), Index: CPU E5-2680 v3 (48 vCPU)
Memory = Data: 64 GB, Index: 512 GB
Disk = Samsung PM863 SSD
