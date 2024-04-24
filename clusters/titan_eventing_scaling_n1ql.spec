[clusters]
titan =
    172.23.96.100:kv,n1ql
    172.23.96.101:kv,n1ql
    172.23.96.102:kv,n1ql
    172.23.96.103:kv,n1ql
    172.23.96.104:kv,n1ql
    172.23.96.105:kv,n1ql
    172.23.96.106:eventing
    172.23.96.107:eventing
    172.23.96.108:eventing
    172.23.96.109:eventing

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
