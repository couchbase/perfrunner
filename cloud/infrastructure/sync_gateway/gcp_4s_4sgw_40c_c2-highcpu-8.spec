[infrastructure]
provider = gcp
type = gce

[clusters]
couchbase1 =
        gce.gce_cluster_1.gce_node_group_1.1:kv,index,n1ql
        gce.gce_cluster_1.gce_node_group_1.2:kv,index,n1ql
        gce.gce_cluster_1.gce_node_group_1.3:kv,index,n1ql
        gce.gce_cluster_1.gce_node_group_1.4:kv,index,n1ql

[syncgateways]
syncgateways1 = 
        gce.gce_cluster_1.gce_node_group_2.1
        gce.gce_cluster_1.gce_node_group_2.2
        gce.gce_cluster_1.gce_node_group_2.3
        gce.gce_cluster_1.gce_node_group_2.4

[clients]
workers1 =
        gce.gce_cluster_1.gce_node_group_3.1
        gce.gce_cluster_1.gce_node_group_3.2
        gce.gce_cluster_1.gce_node_group_3.3
        gce.gce_cluster_1.gce_node_group_3.4
        gce.gce_cluster_1.gce_node_group_3.5
        gce.gce_cluster_1.gce_node_group_3.6
        gce.gce_cluster_1.gce_node_group_3.7
        gce.gce_cluster_1.gce_node_group_3.8
        gce.gce_cluster_1.gce_node_group_3.9
        gce.gce_cluster_1.gce_node_group_3.10
        gce.gce_cluster_1.gce_node_group_3.11
        gce.gce_cluster_1.gce_node_group_3.12
        gce.gce_cluster_1.gce_node_group_3.13
        gce.gce_cluster_1.gce_node_group_3.14
        gce.gce_cluster_1.gce_node_group_3.15
        gce.gce_cluster_1.gce_node_group_3.16
        gce.gce_cluster_1.gce_node_group_3.17
        gce.gce_cluster_1.gce_node_group_3.18
        gce.gce_cluster_1.gce_node_group_3.19
        gce.gce_cluster_1.gce_node_group_3.20
        gce.gce_cluster_1.gce_node_group_3.21
        gce.gce_cluster_1.gce_node_group_3.22
        gce.gce_cluster_1.gce_node_group_3.23
        gce.gce_cluster_1.gce_node_group_3.24
        gce.gce_cluster_1.gce_node_group_3.25
        gce.gce_cluster_1.gce_node_group_3.26
        gce.gce_cluster_1.gce_node_group_3.27
        gce.gce_cluster_1.gce_node_group_3.28
        gce.gce_cluster_1.gce_node_group_3.29
        gce.gce_cluster_1.gce_node_group_3.30
        gce.gce_cluster_1.gce_node_group_3.31
        gce.gce_cluster_1.gce_node_group_3.32
        gce.gce_cluster_1.gce_node_group_3.33
        gce.gce_cluster_1.gce_node_group_3.34
        gce.gce_cluster_1.gce_node_group_3.35
        gce.gce_cluster_1.gce_node_group_3.36
        gce.gce_cluster_1.gce_node_group_3.37
        gce.gce_cluster_1.gce_node_group_3.38
        gce.gce_cluster_1.gce_node_group_3.39
        gce.gce_cluster_1.gce_node_group_3.40

[utilities]
profile = default

[gce]
clusters = gce_cluster_1

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_2,gce_node_group_3
storage_class = pd-extreme

[gce_node_group_1]
instance_type = n2d-standard-16
instance_capacity = 4
volume_size = 1000
volume_type = pd-extreme
iops = 30000

[gce_node_group_2]
instance_type = c2d-highcpu-8
instance_capacity = 4
volume_size = 1000
volume_type = pd-extreme
iops = 50000

[gce_node_group_3]
instance_type = c2-standard-8
instance_capacity = 40
volume_size = 1000
volume_type = pd-extreme
iops = 10000

[storage]
data = /data
backup = gs://perftest-gcp-backup

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = 16vCPU
Memory = 64GB
Disk = pd-extreme 1TB 20000 IOPS
