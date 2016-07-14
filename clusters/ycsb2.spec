[clusters]
ycsb2 =
    172.23.100.190:8091
    172.23.100.191:8091
    172.23.100.192:8091
    172.23.100.193:8091
    172.23.100.204:8091
    172.23.100.205:8091
    172.23.100.206:8091
    172.23.100.207:8091
    172.23.100.240:8091,index


[clients]
hosts =
    172.23.100.194
credentials = root:couchbase


[storage]
data = /data
index = /data


[credentials]
rest = Administrator:password
ssh = root:couchbase


[parameters]
Platform = HW
OS = CentOS 7
CPU = E5-2680 v3 (48 vCPU)
Memory = Data: 64GB, Query & Index: 256GB
Disk = SSD
