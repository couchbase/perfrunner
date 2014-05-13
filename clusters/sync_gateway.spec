[clusters]
sync_gateway =
    172.23.96.64:8091
    172.23.96.65:8091
    172.23.96.62:8091

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[settings]
num_gateways = 0

[gateways]
hosts =
    172.23.96.63
    172.23.96.66
    172.23.96.67
    172.23.96.68

[gateloads]
hosts =
    172.23.100.30
    172.23.100.31
    172.23.100.32
    172.23.100.34
