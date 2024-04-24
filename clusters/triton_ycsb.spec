[clusters]
triton =
    triton-srv-05-ip6.perf.couchbase.com:kv,index,n1ql

[clients]
hosts =
    triton-cnt-01.perf.couchbase.com
credentials = root:couchbase

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = Ubuntu 20.04
CPU = E5-2680 v3 (48 vCPU)
Memory = 256GB
Disk = Samsung SM863
