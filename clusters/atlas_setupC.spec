[clusters]
fts =
    172.23.99.211:kv,index,n1ql,fts
    172.23.99.39:kv,index,n1ql,fts
    172.23.99.40:kv,index,n1ql,fts

[clients]
hosts =
    172.23.99.210
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2680 v3 (48 vCPU)
Memory = 256 GB
Disk = Samsung PM863
