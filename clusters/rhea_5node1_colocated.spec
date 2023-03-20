[clusters]
rhea =
    172.23.97.21:kv,n1ql,index
    172.23.97.22:kv,n1ql,index
    172.23.97.23:kv,n1ql,index
    172.23.97.24:kv,n1ql,index
    172.23.97.25:kv,n1ql,index

[clients]
hosts =
    172.23.97.32
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2680 v4 2.40GHz (56 vCPU)
Memory = 64 GB
Disk = Samsung SSD 883 x3, RAID0
