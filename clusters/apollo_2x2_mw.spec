[clusters]
apollo_c1 =
    172.23.96.15:8091
    172.23.96.16:8091
apollo_c2 =
    172.23.96.17:8091
    172.23.96.18:8091

[workers]
apollo_w1 =
    172.23.96.10
apollo_w2 =
    172.23.96.10

[storage]
data = /data
index = /data2

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = CentOS 6.4 64-bit
CPU = Intel Xeon CPU E5-2630
Memory = 128 GB
Disk = 2 x HDD
