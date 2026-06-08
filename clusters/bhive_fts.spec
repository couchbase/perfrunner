[clusters]
bhive =
    172.23.100.85:kv
    172.23.100.86:kv
    172.23.100.88:kv,fts
    172.23.100.87:index,n1ql

[clients]
hosts =
    172.23.100.89

[storage]
data = /data

[metadata]
cluster = bhive
