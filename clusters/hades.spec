[clusters]
hades =
    172.23.100.70:8091
    172.23.100.71:8091
    172.23.100.72:8091
    172.23.100.73:8091
    172.23.100.55:8091,n1ql
    172.23.100.45:8091,index

[clients]
hosts =
    172.23.100.44
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
Memory = Data: 64GB, Query & Index: 256GB
Disk = SSD
