[clusters]
xdcr_vms_c1 =
    172.23.97.48:8091
xdcr_vms_c2 =
    172.23.97.53:8091

[workers]
xdcr_vms_w1 =
    172.23.97.47
xdcr_vms_w2 =
    172.23.97.52

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = VM
OS = CentOS 6.3 64-bit
CPU = Intel Xeon X5650
Memory = 28 GB
Disk = 1 x HDD
