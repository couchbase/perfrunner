[clusters]
iris =
    172.23.100.210:kv
    172.23.100.211:kv
    172.23.100.212:kv
    172.23.100.213:kv
    172.23.100.55:n1ql
    172.23.100.45:index
    172.23.100.40:n1ql

[clients]
hosts =
    172.23.100.44
    172.23.100.104
    172.23.100.105

[storage]
data = /data

[parameters]
OS = Ubuntu 20.04
CPU = Data: E5-2680 v3 (48 vCPU), Query & Index: E5-2680 v3 (48 vCPU)
Memory = Data & Query: 64GB, Index: 512GB
Disk = Samsung Pro 850
