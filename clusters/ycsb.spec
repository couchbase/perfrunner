[clusters]
ycsb = 172.23.123.38:8091


[clients]
hosts =
    172.23.123.40
credentials = root:couchbase

[storage]
data = /opt/couchbase/data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = CentOS 6.5
CPU = Intel(R) Xeon(R) CPU   X5650  @ 2.67GHz 24 core CPU
Memory = 128 GB
Disk = SSD