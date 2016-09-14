[clusters]
hestia =
    172.23.99.203:8091
    172.23.99.204:8091
    172.23.99.205:8091
    172.23.99.206:8091

[clients]
hosts =
    172.23.99.200
credentials = root:couchbase

[storage]
data = /data
index = /index

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = HW
OS = CentOS 7
CPU = E5-2630 (24 vCPU)
Memory = 64GB
Disk = SSD
