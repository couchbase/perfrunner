[clusters]
rhea =
    172.23.97.21:kv
    172.23.97.22:kv
    172.23.97.23:kv,n1ql
    172.23.97.24:kv,n1ql
    172.23.97.25:kv,n1ql
    172.23.97.26:kv,n1ql
    172.23.97.129:index
    172.23.97.130:index
    172.23.97.131:index
    172.23.97.132:eventing

[syncgateways]
syncgateways =
    172.23.97.27
    172.23.97.28
    172.23.97.29
    172.23.97.30

[clients]
hosts =
    172.23.97.32
    172.23.97.33

[storage]
data = /data

[parameters]
OS = Ubuntu 20
CPU = E5-2680 v4 2.40GHz (56 vCPU)
Memory = 64 GB
Disk = Samsung SSD 883 x3, RAID0
