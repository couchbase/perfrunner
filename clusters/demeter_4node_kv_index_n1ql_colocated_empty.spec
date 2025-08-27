[clusters]
demeter =
    172.23.100.161:kv,n1ql,index
    172.23.100.162:kv,n1ql,index
    172.23.100.163:kv,n1ql,index
    172.23.100.9:empty

[clients]
hosts =
    172.23.100.165

[storage]
data = /data
index = /data

[metadata]
cluster = demeter
