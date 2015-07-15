[clusters]
secondary =
    172.23.120.18:8091
    172.23.120.19:8091
    172.23.100.16:8091,index
    172.23.120.20:8091
    172.23.120.21:8091

[clients]
hosts =
    172.23.100.56
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = CentOS 6.5
CPU = Intel Xeon E5-2680 v2 (40 vCPU)
Memory = 32 GB
Disk = RAID 10 SSD
