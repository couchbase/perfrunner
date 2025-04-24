[infrastructure]
provider = capella
backend = gcp

[clusters]
couchbase1 =
    gce.gce_cluster_1.gce_node_group_1.1:kv
    gce.gce_cluster_1.gce_node_group_1.2:kv
    gce.gce_cluster_1.gce_node_group_1.3:kv
    gce.gce_cluster_1.gce_node_group_1.4:kv
    gce.gce_cluster_1.gce_node_group_2.1:index,n1ql
    gce.gce_cluster_1.gce_node_group_2.2:index,n1ql
    gce.gce_cluster_1.gce_node_group_2.3:index,n1ql
    gce.gce_cluster_1.gce_node_group_2.4:index,n1ql

[clients]
workers1 =
    gce.gce_cluster_1.gce_node_group_3.1

[utilities]
profile = default

[gce]
clusters = gce_cluster_1

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_2,gce_node_group_3
storage_class = pd-ssd

[gce_node_group_1]
instance_type = n2-highmem-8
instance_capacity = 4
volume_size = 1000
volume_type = pd-ssd

[gce_node_group_2]
instance_type = n2-highmem-32
instance_capacity = 4
volume_size = 2000
volume_type = pd-ssd

[gce_node_group_3]
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
OS = Ubuntu 20.04
CPU = Data: n2-highmem-16 (16vCPU), Index/Query: n2-highmem-32 (32vCPU)
Memory = Data: 128GB, Index/Query: 256GB
Disk = pd-ssd, Data: 1TB 25000 IOPS, Index/Query: 1.5TB 51000 IOPS
