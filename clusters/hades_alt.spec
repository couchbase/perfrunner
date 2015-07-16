[clusters]
hades =
    172.23.100.70:8091,kv,n1ql,index
    172.23.100.71:8091,kv,n1ql,index
    172.23.100.72:8091,kv,n1ql,index
    172.23.100.73:8091,kv,n1ql,index

[clients]
hosts =
    172.23.100.44
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = Centos 6.5 (Final)
CPU = Intel Xeon E5-2630 v2 (2.50GHz)(24 cores)
Memory = 64GB
Disk = SDD