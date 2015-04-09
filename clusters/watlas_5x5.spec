[clusters]
atlas_c1 =
    172.23.100.17:8091
    172.23.100.18:8091
    172.23.100.19:8091
    172.23.100.20:8091
    172.23.100.21:8091
atlas_c2 =
    172.23.100.22:8091
    172.23.100.23:8091
    172.23.100.24:8091
    172.23.100.25:8091
    172.23.100.26:8091

[clients]
hosts =
    172.23.100.28
    172.23.100.27
credentials = root:couchbase

[storage]
data = d:\data
index = c:\data

[credentials]
rest = Administrator:password
ssh = Administrator:Membase123


[parameters]
Platform = Physical
OS = Windows Server 2012
CPU = Intel Xeon E5-2680 v2 (40 vCPU)
Memory = 256 GB
Disk = RAID 10 SSD
