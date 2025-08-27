[clusters]
hebe =
    172.23.100.190:kv,n1ql
    172.23.100.191:kv,n1ql
    172.23.100.192:kv,n1ql
    172.23.100.193:index,n1ql

[syncgateways]
syncgateways =
    172.23.100.204
    172.23.100.205
    172.23.100.206
    172.23.100.207

[clients]
hosts =
    172.23.97.250
    172.23.97.251
    172.23.97.252
    172.23.97.253

[storage]
data = /data
index = /data

[metadata]
cluster = hebe
