[clusters]
hebe_2 =
    172.23.100.204:kv,index,n1ql
    172.23.100.205:kv,index,n1ql
    172.23.100.206:kv,index,n1ql

[syncgateways]
syncgateways =
    172.23.100.207

[clients]
hosts =
    172.23.97.250
    172.23.97.251
    172.23.97.252
    172.23.97.253
    172.23.98.8
    172.23.100.194

[storage]
data = /data
index = /data

[metadata]
cluster = hebe
