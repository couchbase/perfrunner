[clusters]
magma-nvme =
    172.23.103.51:kv
    172.23.103.52:kv
    172.23.103.53:kv
    172.23.103.54:kv

[clients]
hosts =
    172.23.97.41
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Gold 6230 2.1GHz (80 vCPU)
Memory = 128 GB
Disk = Intel P4610 3.2TB NVMe x3, RAID0
