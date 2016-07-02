[clusters]
hestia =
    172.23.99.203:8091
    172.23.99.204:8091
    172.23.99.205:8091
    172.23.99.206:8091
    172.23.99.201:8091,index
    172.23.99.202:8091,n1ql

[clients]
hosts =
    172.23.99.200
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = HW
OS = CentOS 7
CPU = Data: E5-2630 (24 vCPU), Query & Index: E5-2680 v3 (48 vCPU)
Memory = Data: 64GB, Query & Index: 256GB
Disk = SSD
