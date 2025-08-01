[clusters]
hebe_c1 =
    172.23.100.190:kv,index,n1ql
    172.23.100.191:kv,index,n1ql
hebe_c2 =
    172.23.100.204:kv,index,n1ql
    172.23.100.205:kv,index,n1ql


[clients]
hosts =
    172.23.97.250
    172.23.97.251

[storage]
data = /data
index = /data
backup = /data/workspace/backup

[parameters]
OS = Ubuntu 20.04
CPU = E5-2680 v3 (48 vCPU)
Memory = 64GB
Disk = Samsung Pro 850
