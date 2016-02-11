[clusters]
hades =
    10.17.0.107:8091
    10.17.0.105:8091,n1ql
    10.17.0.106:8091,index


[clients]
hosts =
    10.5.3.40
credentials = root:couchbase

[storage]
data = /opt/couchbase/var/lib/couchbase/data
index = /opt/couchbase/var/lib/couchbase/data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = Centos 6.5 (Final)
CPU = Intel Xeon E5-2630 v2 (2.50GHz)(24 cores)(Data), Intel Xeon E5-2680 v3 (2.60GHz)(48 cores)(Query, Index)
Memory = 64GB (Data), 256GB (Index, Query)
Disk = SDD
