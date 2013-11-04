[clusters]
tahoe =
    hummer-s10901.sc.couchbase.com:8091
    hummer-s10902.sc.couchbase.com:8091
    hummer-s10903.sc.couchbase.com:8091
    hummer-s10904.sc.couchbase.com:8091

[storage]
data = /data
index = /data1

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = VM
OS = CentOS 6.3 64-bit
CPU = Intel Xeon CPU X5650@ 2.67GHz (4 core)
Memory = 16 GB
Disk = 1 x HDD (LVM)
