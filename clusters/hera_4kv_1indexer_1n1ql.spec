[clusters]
hera =
    172.23.96.117:8091
    172.23.96.118:8091
    172.23.96.119:8091
    172.23.96.120:8091
    172.23.96.123:8091,index
    172.23.96.112:8091,n1ql

[clients]
hosts =
    172.23.99.111
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
CPU = Data: CPU E5-2630 v3 (32 vCPU), Query & Index: E5-2680 v3 (48 vCPU)
Memory = Data: 64GB, Query & Index: 256GB
Disk = SSD
