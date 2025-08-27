[clusters]
oceanus =
    172.23.96.5:kv
    172.23.96.7:kv
    172.23.96.8:index,n1ql
    172.23.96.9:index,n1ql
    172.23.96.57:index,n1ql
    172.23.96.205:index,n1ql

[clients]
hosts =
    172.23.96.22

[storage]
data = /data
index = /data1

[metadata]
cluster = oceanus
