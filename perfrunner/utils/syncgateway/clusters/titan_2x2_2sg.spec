[clusters]
titan_c1 =
    172.23.96.100:kv,index,n1ql
    172.23.96.101:kv,index,n1ql
titan_c2 =
    172.23.96.102:kv,index,n1ql
    172.23.96.103:kv,index,n1ql

[syncgateways]
syncgateways1 =
    172.23.96.104
syncgateways2 =
    172.23.96.105

[clients]
hosts =
    172.23.97.208
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = Ubuntu 20
CPU = E5-2680 v3 (48 vCPU)
Memory = 256 GB
Disk = Samsung PM863a
