[clusters]
rhea_c1 =
    172.23.97.21:kv
    172.23.97.22:kv
    172.23.97.23:kv
    172.23.97.24:kv
    172.23.97.25:kv
rhea_c2 =
    172.23.97.26:kv
    172.23.97.27:kv
    172.23.97.28:kv
    172.23.97.29:kv
    172.23.97.30:kv

[clients]
hosts =
    172.23.97.32
    172.23.97.33
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2680 v4 2.40GHz (56 vCPU)
Memory = 64 GB
Disk = Samsung SSD 883 x3, RAID0
