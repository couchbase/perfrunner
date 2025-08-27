[clusters]
oceanus =
    172.23.96.5:kv,index,n1ql
    172.23.96.7:kv,index,n1ql
    172.23.96.8:cbas
    172.23.96.9:cbas
    172.23.96.57:cbas
    172.23.96.205:cbas

[clients]
hosts =
    172.23.96.22

[storage]
data = /data
analytics = /data1 /data2

[metadata]
cluster = oceanus
