[clusters]
oceanus =
    172.23.96.5:kv,index,n1ql
    172.23.96.57:cbas
    172.23.96.205:cbas

[clients]
hosts =
    172.23.96.22
credentials = root:couchbase

[storage]
data = /data
analytics = /data1 /data2

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2680 v3 (24 cores)
Memory = 64 GB
Disk = 2 x Samsung SM863
