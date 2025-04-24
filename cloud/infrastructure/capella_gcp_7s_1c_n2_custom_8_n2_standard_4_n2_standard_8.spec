[infrastructure]
provider = capella
backend = gcp

[clusters]
couchbase1 =
    gce.gce_cluster_1.gce_node_group_1.1:kv
    gce.gce_cluster_1.gce_node_group_1.2:kv
    gce.gce_cluster_1.gce_node_group_1.3:kv
    gce.gce_cluster_1.gce_node_group_2.1:index
    gce.gce_cluster_1.gce_node_group_2.2:index
    gce.gce_cluster_1.gce_node_group_3.1:n1ql
    gce.gce_cluster_1.gce_node_group_3.2:n1ql

[clients]
workers1 =
    gce.gce_cluster_1.gce_node_group_4.1

[utilities]
profile = default

[gce]
clusters = gce_cluster_1

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_2,gce_node_group_3,gce_node_group_4
storage_class = pd-ssd

[gce_node_group_1]
instance_type = n2-custom-8-16384
instance_capacity = 3
volume_size = 300
volume_type = pd-ssd

[gce_node_group_2]
instance_type = n2-standard-4
instance_capacity = 2
volume_size = 300
volume_type = pd-ssd

[gce_node_group_3]
instance_type = n2-standard-8
instance_capacity = 2
volume_size = 300
volume_type = pd-ssd

[gce_node_group_4]
instance_type = n2-standard-64
instance_capacity = 1
volume_size = 100
volume_type = pd-ssd

[storage]
data = /data
backup = gs://perftest-gcp-backup

[metadata]
source = default_capella

[parameters]
OS = Ubuntu 20
CPU = Data: n2-custom-8-16384 (8vCPU), Index: n2-standard-4 (4vCPU), Query: c2d-highcpu-8 (8vCPU)
Memory = Data: 16GB, Index: 16GB, Query: 16GB
Disk = pd-ssd