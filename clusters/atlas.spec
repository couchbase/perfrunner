[clusters]
atlas =
    172.23.100.17:8091
    172.23.100.18:8091
    172.23.100.19:8091
    172.23.100.20:8091
    172.23.100.21:8091
    172.23.100.22:8091
    172.23.100.23:8091
    172.23.100.24:8091
    172.23.100.25:8091
    172.23.100.26:8091

[clients]
hosts =
    172.23.105.215
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
CPU = Intel Xeon E5-2680 v2
Memory = 256 GB
Disk = RAID 10 SSD
