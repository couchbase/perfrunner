[clusters]
test_n1ql =
    172.23.121.113:8091,kv,n1ql
    172.23.121.114:8091,index
    172.23.107.95:8091

[clients]
hosts =
    172.23.107.44
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = VM
OS = Centos 6.6
CPU = Intel Xeon X-5650 (4 cores)
Memory = 4GB
Disk = HDD