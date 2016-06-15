[clusters]
hades =
    172.23.100.70:8091,kv,n1ql,index
    172.23.100.71:8091,kv,n1ql,index
    172.23.100.72:8091,kv,n1ql,index
    172.23.100.73:8091,kv,n1ql,index

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
CPU = E5-2630 v2 (24 vCPU)
Memory = 64GB
Disk = SSD
