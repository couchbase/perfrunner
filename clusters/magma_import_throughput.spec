[clusters]
magma =
    172.23.97.21:kv,index,n1ql
    172.23.97.22:kv,index,n1ql
    172.23.97.23:kv,index,n1ql
    172.23.97.24:kv,index,n1ql
    172.23.97.25:kv,index,n1ql
    172.23.97.26:kv,index,n1ql
    172.23.97.27:kv,index,n1ql
    172.23.97.28:kv,index,n1ql
    172.23.97.29:kv,index,n1ql
    172.23.97.30:kv,index,n1ql

[clients]
hosts =
    172.23.97.36
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2680 v3 (16 vCPU)
Memory = 32GB
Disk = Samsung DCT883
