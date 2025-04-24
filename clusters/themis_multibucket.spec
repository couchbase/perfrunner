[clusters]
source_cluster =
    172.23.99.157:kv
    172.23.99.158:kv
    172.23.99.159:kv
    172.23.96.19:index,n1ql
    172.23.96.15:index,n1ql
    172.23.97.177:eventing
    172.23.96.23:cbas
    172.23.96.20:fts
    172.23.99.160:kv
    172.23.99.161:kv
destination_bucket =
    172.23.96.16:kv
    172.23.96.17:kv

[clients]
hosts =
    172.23.99.247

[storage]
data = /data

[parameters]
OS = Ubuntu 20.04
CPU = E5-2680 v3 (24 vCPU)
Memory = 64GB
Disk = Samsung Pro 850