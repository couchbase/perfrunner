[clusters]
apollo_ssd =
    172.23.96.15:8091
    172.23.96.16:8091
    172.23.96.17:8091
    172.23.96.18:8091

[clients]
hosts =
    172.23.97.105
credentials = root:couchbase

[storage]
data = /ssd
index = /ssd2

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = CentOS 6.5
CPU = Intel Xeon E5-2630
Memory = 64 GB
Disk = 2 x SSD
