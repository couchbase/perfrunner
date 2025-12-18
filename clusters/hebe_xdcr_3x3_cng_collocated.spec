[clusters]
hebe_c1 =
    172.23.100.190:kv,index,n1ql
    172.23.100.191:kv,index,n1ql
    172.23.100.192:kv,index,n1ql
hebe_c2 =
    172.23.100.204:kv,index,n1ql
    172.23.100.205:kv,index,n1ql
    172.23.100.206:kv,index,n1ql

[stellar_gateways]
hebe_c2 =
    172.23.100.204
    172.23.100.205
    172.23.100.206

[clients]
hosts =
    172.23.97.250
    172.23.97.251

[storage]
data = /data
index = /data
backup = /data/workspace/backup

[metadata]
cluster = hebe
