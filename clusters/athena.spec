[clusters]
atlas =
    172.23.120.12:8091
    172.23.120.13:8091
    172.23.120.14:8091
    172.23.120.15:8091

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
Platform = VM
OS = CentOS 5.10
CPU = Intel Xeon E5-2620 (12 cores)
Memory = 64 GB
Disk = SSD
