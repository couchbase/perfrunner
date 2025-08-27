[clusters]
nyx =
    172.23.97.3:kv
    172.23.97.4:kv
    172.23.97.10:index
    172.23.97.5:kv
    172.23.97.6:kv
    172.23.97.7:kv

[clients]
hosts =
    172.23.97.9

[storage]
data = /nvme
index = /nvme

[metadata]
cluster = nyx

[parameters]
Disk = Samsung PM1725 SSD
