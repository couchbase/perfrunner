[clusters]
zeus =
    172.23.96.25
    172.23.96.26
    172.23.96.27
    172.23.96.28
    172.23.100.210,n1ql
    172.23.100.211,index

[clients]
hosts =
    172.23.100.212
credentials = root:couchbase

[storage]
data = f:\data
index = e:\data

[credentials]
rest = Administrator:password
ssh = Administrator:Membase123

[parameters]
OS = Windows Server 2012
CPU = Data: E5-2630 v2 (24 vCPU), Query & Index: E5-2680 v3 (48 vCPU)
Memory = Data: 64GB, Query & Index: 256GB
Disk = SSD
