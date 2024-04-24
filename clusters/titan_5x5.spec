[clusters]
titan_c1 =
    172.23.96.105:kv
    172.23.96.106:kv
    172.23.96.107:kv
    172.23.96.108:kv
    172.23.96.109:kv
titan_c2 =
    172.23.96.100:kv
    172.23.96.101:kv
    172.23.96.102:kv
    172.23.96.103:kv
    172.23.96.104:kv

[clients]
hosts =
    172.23.97.208
    172.23.97.209
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = Ubuntu 20.04
CPU = E5-2680 v3 (48 vCPU)
Memory = 256 GB
Disk = Samsung PM863a
