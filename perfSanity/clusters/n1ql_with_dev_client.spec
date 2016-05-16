[clusters]
perfregression =
    10.17.0.105:8091
    10.17.0.106:8091,index
    10.17.0.107:8091,n1ql

[clients]
hosts =
    10.1.5.26
credentials = root:couchbase

[storage]
data = /opt/couchbase/var/lib/couchbase/data
index = /opt/couchbase/var/lib/couchbase/data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = VM
OS = Centos 6.6
CPU = Intel Xeon X-5650 (4 cores)
Memory = 20GB
Disk = HDD
