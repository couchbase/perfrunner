[clusters]
perfregression =
    10.1.5.23:8091
    10.1.5.24:8091,index
    10.1.5.25:8091,n1ql


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
Platform = Physical
OS = CentOS 6.5
CPU = Intel Xeon E5-2630 (24 vCPU)
Memory = 7 GB
Disk = RAID 10 HDD
