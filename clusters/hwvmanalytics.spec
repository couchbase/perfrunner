[clusters]
hwcluster =
    172.23.96.6:kv
    172.23.96.19:cbas

[clients]
hosts =
    172.23.96.18
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Intel(R) Xeon(R) CPU E5-2680 v3 (16 vCPU)
Memory = 32 GB
Disk = Unknown VM
