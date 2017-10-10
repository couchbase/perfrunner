[clusters]
hwcluster =
    172.23.98.29:kv,index,n1ql
    172.23.98.30:cbas
    172.23.98.31:cbas

[clients]
hosts =
    172.23.98.32
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = Ubuntu 16.04
CPU = Intel Xeon E5-2630 v4  (4 vCPU)
Memory = 4 GB
Disk = Unknown VM

