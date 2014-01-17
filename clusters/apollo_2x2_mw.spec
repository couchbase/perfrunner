[clusters]
apollo_c1 =
    172.23.96.15:8091
    172.23.96.16:8091
apollo_c2 =
    172.23.96.17:8091
    172.23.96.18:8091

[workers]
apollo_w1 =
    172.23.97.74
apollo_w2 =
    172.23.97.75

[storage]
data = /data
index = /data2

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = CentOS 6.5
CPU = Intel Xeon E5-2630
Memory = 64 GB
Disk = 2 x HDD
