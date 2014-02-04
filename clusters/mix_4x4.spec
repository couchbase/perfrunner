[clusters]
apollo =
    172.23.96.15:8091
    172.23.96.16:8091
    172.23.96.17:8091
    172.23.96.18:8091
thor =
    172.23.96.11:8091
    172.23.96.12:8091
    172.23.96.13:8091
    172.23.96.14:8091

[clients]
hosts =
    172.23.97.74
    172.23.97.104
credentials = root:couchbase

[storage]
data = /data
index = /data2

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = CentOS 6.5
CPU = Intel Xeon E5-2630
Memory = 64 GB
Disk = 2 x HDD
