# Cluster config for use with the kv_max_ops tests
# Two server nodes and two server nodes repurposed into clients.
# Allows for much higher ops/s, to be used for pillowfight max_throughput tests

[clusters]
hermes =
    172.23.100.180:8091
    172.23.100.181:8091

[clients]
hosts =
    172.23.100.182
    172.23.100.183
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = CentOS 6.5
CPU = Intel Xeon E5-2630 (24 vCPU)
Memory = 64 GB
Disk = RAID 10 HDD
