[clusters]
bhive =
    172.23.100.81:kv
    172.23.100.83:kv
    172.23.100.84:kv
    172.23.100.82:index,n1ql

[clients]
hosts =
    172.23.100.154

[storage]
data = /data

[metadata]
cluster = bhive
