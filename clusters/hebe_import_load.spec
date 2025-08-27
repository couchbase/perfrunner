[clusters]
hebe =
    172.23.100.190:kv
    172.23.100.191:kv
    172.23.100.192:kv
    172.23.100.193:n1ql
    172.23.100.207:index

[clients]
hosts =
    172.23.100.194

[storage]
data = /data
index = /data

[metadata]
cluster = hebe
