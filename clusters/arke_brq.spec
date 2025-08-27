[clusters]
arke_basic =
    172.23.97.12:kv
    172.23.97.13:kv
    172.23.97.14:kv
    172.23.97.19:index,n1ql
    172.23.97.20:index,n1ql
    172.23.97.15:kv

[clients]
hosts =
    172.23.97.16
    172.23.97.17

[storage]
data = /data

[metadata]
cluster = arke
