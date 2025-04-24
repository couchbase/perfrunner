[clusters]
rhea =
    172.23.97.21:kv
    172.23.97.22:kv
    172.23.97.23:kv
    172.23.97.24:kv
    172.23.97.129:fts
    172.23.97.130:fts
    172.23.97.131:fts

[clients]
hosts =
    172.23.97.32

[storage]
data = /data
backup = /workspace/backup

[parameters]
OS = Ubuntu 20.04
CPU = E5-2680 v4 2.40GHz (Data: 56 vCPU, FTS: 80 vCPU)
Memory = Data: 64GB, FTS: 256GB
Disk = Samsung SSD 883 x3, RAID0