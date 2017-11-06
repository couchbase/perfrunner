[clusters]
triton =
    triton-srv-05.perf.couchbase.com:kv,index,n1ql

[clients]
hosts =
    triton-cnt-01.perf.couchbase.com
credentials = root:couchbase

[storage]
data = /data
index = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = E5-2680 v3 (48 vCPU)
Memory = 256GB
Disk = Samsung SM863
