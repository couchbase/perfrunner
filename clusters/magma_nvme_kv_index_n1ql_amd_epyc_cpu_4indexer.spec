[clusters]
magma-nvme =
    172.23.100.135:kv
    172.23.100.136:kv
    172.23.100.137:kv
    172.23.100.80:index,n1ql
    172.23.100.81:index,n1ql
    172.23.100.82:index,n1ql
    172.23.100.83:index,n1ql

[clients]
hosts =
    172.23.100.154
credentials = root:couchbase

[storage]
data = /data

[parameters]
OS = Ubuntu 20.04
CPU = Data: Gold 6230 2.1GHz (80 vCPU), Index: AMD EPYC 7643 (192 vCPU)
Memory = Data: 256 GB, Index: 256 GB
Disk = Data: Intel P4610 3.2TB NVMe x3, RAID0; IndexSamsung PM1743 15.36TB
