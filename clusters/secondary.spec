[clusters]
secondary =
    172.23.99.241:8091
    172.23.99.245:8091
    172.23.99.246:8091
    172.23.99.247:8091,index
    172.23.99.251:8091

[clients]
hosts =
    172.23.99.250
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
