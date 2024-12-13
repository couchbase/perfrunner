[clusters]
hemera =
    172.23.100.81:kv
    172.23.100.83:kv
    172.23.100.84:kv
    172.23.100.82:index,n1ql

[clients]
hosts =
    172.23.100.154
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = Ubuntu 20.04
CPU = Data/Index: AMD EPYC 7643 (192 vCPU)
Memory = Data/Index: 256 GB
Disk = Samsung PM1743 15.36TB
