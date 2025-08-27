[clusters]
rhea =
    172.23.97.21:kv,n1ql,index
    172.23.97.22:kv,n1ql,index
    172.23.97.23:kv,n1ql,index
    172.23.97.24:kv,n1ql,index
    172.23.97.25:kv,n1ql,index

[clients]
hosts =
    172.23.97.32

[storage]
data = /data

[metadata]
cluster = rhea
