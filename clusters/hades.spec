[clusters]
hades =
    172.23.100.70:8091
    172.23.100.71:8091
    172.23.100.72:8091
    172.23.100.73:8091
    172.23.100.55:8091,n1ql
    172.23.100.45:8091,index

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
CPU = Intel Xeon E5-2630 v2 (2.50GHz)(24 cores)(Data), Intel Xeon E5-2680 v3 (2.60GHz)(48 cores)(Query, Index)
Memory = 64GB (Data), 256GB (Index, Query)
Disk = SDD