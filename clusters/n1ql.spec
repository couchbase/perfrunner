[clusters]
atlas =
    172.23.100.17:8091
    172.23.100.18:8091
    172.23.100.19:8091
    172.23.100.20:8091

[n1ql]
query =  
    172.23.100.21:8093

[index]
indexes =  
    172.23.100.22:8093

[clients]
hosts =
    172.23.100.27
    172.23.100.28
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = CentOS 6.5
CPU = Intel Xeon E5-2630 (24 vCPU)
Memory = 64 GB
Disk = 2 x SSD

