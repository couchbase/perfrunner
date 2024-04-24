[clusters]
demeter_c1 =
    172.23.100.161:kv

demeter_c2 =
    172.23.100.162:kv
    
demeter_c3 =
    172.23.100.163:kv

[clients]
hosts =
    172.23.100.165
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = Ubuntu 20.04
CPU = E5-2630 v3 (32 vCPU)
Memory = 64 GB
Disk = Samsung 860 1TB
