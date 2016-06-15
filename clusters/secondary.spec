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
Platform = HW
OS = CentOS 6
CPU = Data: E5-2630 v2 (24 vCPU), Query & Index: E5-2680 v3 (48 vCPU)
Memory = Data: 32GB, Query & Index: 128GB
Disk = SSD
