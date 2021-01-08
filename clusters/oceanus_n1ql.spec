[clusters]
oceanus =
    172.23.96.5:kv
    172.23.96.7:kv
    172.23.96.8:index,n1ql
    172.23.96.9:index,n1ql
    172.23.96.57:index,n1ql
    172.23.96.205:index,n1ql

[clients]
hosts =
    172.23.96.22
credentials = root:couchbase

[storage]
data = /data
index = /data1

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2680 v3 (24 cores)
Memory = 64 GB
Disk = Samsung SM863
