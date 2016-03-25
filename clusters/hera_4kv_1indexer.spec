[clusters]
hera =
    172.23.96.117:8091
    172.23.96.118:8091
    172.23.96.119:8091
    172.23.96.120:8091
    172.23.96.123:8091,index

[clients]
hosts =
    172.23.99.111
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = Centos 6.6
CPU = Intel(R) Xeon(R) CPU E5-2630 v3 @ 2.40GHz
Memory = 64GB
Disk = HDD
