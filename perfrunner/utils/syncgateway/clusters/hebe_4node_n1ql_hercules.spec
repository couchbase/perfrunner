[clusters]
hebe =
    172.23.100.190:kv
    172.23.100.191:kv
    172.23.100.192:kv
    172.23.100.193:kv
    172.23.100.205:n1ql
    172.23.100.206:index

[syncgateways]
syncgateways =
    172.23.100.204
    172.23.100.207

[clients]
hosts =
    172.23.100.129
    172.23.100.130
    172.23.100.131
    172.23.100.132

[storage]
data = /data
index = /data

[metadata]
cluster = hebe
