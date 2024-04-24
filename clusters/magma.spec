[clusters]
magma =
    172.23.97.37:kv
    172.23.97.38:kv
    172.23.97.39:kv
    172.23.97.40:kv

[clients]
hosts =
    172.23.97.36
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = Ubuntu 20.04
CPU = E5-2680 v4 (56 vCPU)
Memory = 256GB
Disk = Samsung PM863
