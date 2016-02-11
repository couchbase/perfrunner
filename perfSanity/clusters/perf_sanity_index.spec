[clusters]
perfregression =
    10.17.0.105:8091
    10.17.0.106:8091
    10.17.0.107:8091,index

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
OS = CentOS 6.5
CPU = Intel Xeon E5-2630 (24 vCPU)
Memory = 64 GB
Disk = RAID 10 HDD
