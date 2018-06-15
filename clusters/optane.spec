[clusters]
optane =
    172.23.124.4:kv,cbas
    172.23.124.38:kv,cbas

[clients]
hosts =
    172.23.96.22
credentials = root:couchbase

[storage]
data = /data1
analytics = /data2 /data3 /data4

[credentials]
rest = Administrator:password
ssh = root:couchbase
