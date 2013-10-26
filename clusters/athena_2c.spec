[clusters]
athena =
    172.23.120.14:8091
    172.23.120.15:8091

[workers]
athena_w1 =
    172.23.97.75
athena_w2 =
    tahoe-s10806.sc.couchbase.com

[storage]
data = /data
index = /data2

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = CentOS 5.9 64-bit
CPU = Intel Xeon CPU E5-2620
Memory = 64 GB
Disk = Samsung SSD
