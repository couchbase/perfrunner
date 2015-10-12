[clusters]
backup =
    172.23.123.18:8091
    172.23.123.19:8091
    172.23.123.20:8091
    172.23.123.21:8091

[clients]
hosts =
    172.23.123.21
credentials = root:couchbase

[storage]
data = /tmp
index = /tmp
backup_path=/tmp/cbbackup_dir

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = CentOS 6.5
CPU = Intel Xeon X5650 @ 2.67GHz(40 vCPU)
Memory = 8 GB
Disk = RAID 10 HDD