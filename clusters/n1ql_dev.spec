[clusters]
n1ql =
    172.23.100.39:8091
    172.23.100.40:8091
    172.23.100.42:8091
    172.23.100.43:8091

[clients]
hosts =
    172.23.100.38
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = CentOS 6.5
CPU = Intel Xeon E5-2630 (24 vCPU)
Memory = 64 GB
Disk = 2 x SSD
