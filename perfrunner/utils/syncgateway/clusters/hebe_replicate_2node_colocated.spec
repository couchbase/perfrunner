[clusters]
hebe_c1 =
    172.23.100.190:kv,index,n1ql
hebe_c2 =
    172.23.100.204:kv,index,n1ql

[syncgateways]
syncgateways =
    172.23.100.191
    172.23.100.192
    172.23.100.193
    172.23.100.205
    172.23.100.206
    172.23.100.207

[clients]
hosts =
    172.23.100.194

[storage]
data = /data
index = /data

[metadata]
cluster = hebe
