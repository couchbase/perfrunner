[clusters]
whera =
    172.23.96.117:8091
    172.23.96.118:8091
    172.23.96.119:8091
    172.23.96.120:8091
    172.23.96.112:8091,n1ql
    172.23.96.123:8091,index

[clients]
hosts =
    172.23.97.130
credentials = root:couchbase

[storage]
data = g:\data
index = g:\data

[credentials]
rest = Administrator:password
ssh = Administrator:Membase123

[parameters]
Platform = Physical
OS = Windows Server 2012
CPU = Intel(R) Xeon(R) CPU E5-2630 v3 @ 2.40GHz
Memory = 64GB
Disk = SSD
