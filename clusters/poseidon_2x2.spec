[clusters]
poseidon_c1 =
    172.23.132.10:8091
    172.23.132.11:8091

poseidon_c2 =
    172.23.132.12:8091
    172.23.132.13:8091


[clients]
hosts =
    172.23.132.14
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = HW
OS = CentOS 7
CPU = Data: E5-2630 v2 (24 vCPU)
Memory = Data: 64GB
Disk = SSD
