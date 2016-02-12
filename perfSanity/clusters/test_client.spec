[clusters]
perfregression =
    10.5.3.40:8091


[clients]
hosts =
    10.5.3.40
credentials = root:couchbase

[storage]
data = /opt/couchbase/var/lib/couchbase/data
index = /opt/couchbase/var/lib/couchbase/data
backup_path=/data/cbbackup_dir

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
Platform = Physical
OS = CentOS 6.5
CPU = Intel Xeon E5-2630 (24 vCPU)
Memory = 64 GB
Disk = RAID 10 HDD
