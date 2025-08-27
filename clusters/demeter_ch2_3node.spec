[clusters]
demeter =
    172.23.100.161:kv,index,n1ql
    172.23.100.162:cbas
    172.23.100.163:cbas

[clients]
hosts =
    172.23.100.165

[storage]
data = /data
analytics = /data1

[metadata]
cluster = demeter
