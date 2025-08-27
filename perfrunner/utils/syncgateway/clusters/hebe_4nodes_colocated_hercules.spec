[clusters]
hebe =
    172.23.100.190:kv,index,n1ql
    172.23.100.191:kv,index,n1ql
    172.23.100.192:kv,index,n1ql
    172.23.100.193:kv,index,n1ql

[syncgateways]
syncgateways =
    172.23.100.204
    172.23.100.205
    172.23.100.206
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
