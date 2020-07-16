[clusters]
leto =
    172.23.100.29:kv
    172.23.100.30:kv
    172.23.100.31:kv
    172.23.100.32:kv

[clients]
hosts =
    172.23.100.213
credentials = root:couchbase

[storage]
data = /data
index = /index
backup = /workspace/backup

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2630 (24 vCPU)
Memory = 64 GB
Disk = Samsung Pro 850
