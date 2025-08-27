[clusters]
titan_c1 =
    172.23.96.100:kv,index,n1ql
    172.23.96.101:kv,index,n1ql
titan_c2 =
    172.23.96.102:kv,index,n1ql
    172.23.96.103:kv,index,n1ql

[syncgateways]
syncgateways1 =
    172.23.96.104
syncgateways2 =
    172.23.96.105

[clients]
hosts =
    172.23.97.208

[storage]
data = /data

[metadata]
cluster = titan
