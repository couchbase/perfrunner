[clusters]
hercules =
    172.23.100.121:kv:Group1
    172.23.100.122:kv:Group1
    172.23.100.123:kv:Group1
    172.23.100.124:kv:Group2
    172.23.100.125:kv:Group2
    172.23.100.126:kv:Group2

[clients]
hosts =
    172.23.100.129
    172.23.100.130
    172.23.100.131
    172.23.100.132
    172.23.100.133

[storage]
data = /data

[metadata]
cluster = hercules
