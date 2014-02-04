[clusters]
thor =
    172.23.96.11:8091
    172.23.96.12:8091
    172.23.96.13:8091
    172.23.96.14:8091

[clients]
hosts =
    172.23.97.104
credentials = root:couchbase

[storage]
data = /ssd
index = /ssd

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = CentOS 6.5
CPU = Intel Xeon E5-2630
Memory = 64 GB
Disk = 2 x SSD
