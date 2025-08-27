[clusters]
hebe_c1 =
    172.23.100.190:kv,index,n1ql
    172.23.100.191:kv,index,n1ql
    172.23.100.192:kv,index,n1ql
hebe_c2 =
    172.23.100.204:kv,index,n1ql
    172.23.100.205:kv,index,n1ql
    172.23.100.206:kv,index,n1ql

[syncgateways]
syncgateways1 =
    172.23.100.193
syncgateways2 =
    172.23.100.207

[clients]
hosts =
    172.23.100.194

[storage]
data = /data
index = /data

[metadata]
cluster = hebe
