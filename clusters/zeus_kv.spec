[clusters]
zeus =
    zeus-srv-01.perf.couchbase.com:kv
    zeus-srv-02.perf.couchbase.com:kv
    zeus-srv-03.perf.couchbase.com:kv
    zeus-srv-04.perf.couchbase.com:kv

[clients]
hosts =
    zeus-cnt-01.perf.couchbase.com
credentials = root:couchbase

[storage]
data = e:\data

[credentials]
rest = Administrator:password
ssh = Administrator:Membase123

[parameters]
OS = Windows Server 2012
CPU = E5-2630 v2 (24 vCPU)
Memory = 64 GB
Disk = Samsung Pro 850
