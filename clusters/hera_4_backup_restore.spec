[clusters]
hera =
    172.23.96.117:8091
    172.23.96.118:8091
    172.23.96.119:8091
    172.23.96.120:8091
    172.23.96.123:8091

[clients]
hosts =
    172.23.96.123
credentials = root:couchbase

[storage]
data = /data
index = /data
backup_path=/data/cbbackup_dir

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = HW
OS = CentOS 6
CPU = CPU E5-2630 v3 (32 vCPU)
Memory = 64GB
Disk = SSD
