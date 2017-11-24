[clusters]
hestia =
    172.23.97.177:kv
    172.23.96.16:eventing
    172.23.96.17:eventing
    172.23.96.20:eventing
    172.23.96.23:eventing

[clients]
hosts =
    172.23.96.38
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2630 (24 vCPU)
Memory = 64GB
Disk = Samsung Pro 850
