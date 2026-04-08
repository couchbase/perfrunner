[infrastructure]
provider = capella
backend = gcp

[clusters]
couchbase1 =
    gce.gce_cluster_1.gce_node_group_1.1:kv
    gce.gce_cluster_1.gce_node_group_1.2:kv
    gce.gce_cluster_1.gce_node_group_1.3:kv
    gce.gce_cluster_1.gce_node_group_1.4:kv
    gce.gce_cluster_1.gce_node_group_1.5:kv
    gce.gce_cluster_1.gce_node_group_1.6:kv
    gce.gce_cluster_1.gce_node_group_1.7:kv
    gce.gce_cluster_1.gce_node_group_1.8:kv

couchbase2 =
    gce.gce_cluster_2.gce_node_group_1.1:kv
    gce.gce_cluster_2.gce_node_group_1.2:kv
    gce.gce_cluster_2.gce_node_group_1.3:kv
    gce.gce_cluster_2.gce_node_group_1.4:kv
    gce.gce_cluster_2.gce_node_group_1.5:kv
    gce.gce_cluster_2.gce_node_group_1.6:kv
    gce.gce_cluster_2.gce_node_group_1.7:kv
    gce.gce_cluster_2.gce_node_group_1.8:kv

[clients]
workers1 =
    gce.gce_cluster_1.gce_node_group_3.1

[utilities]
profile = default

[gce]
clusters = gce_cluster_1,gce_cluster_2

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_3
storage_class = pd-ssd

[gce_cluster_2]
node_groups = gce_node_group_1
storage_class = pd-ssd

[gce_node_group_1]
instance_type = n2-highmem-8
instance_capacity = 8
volume_size = 300
volume_type = pd-ssd

[gce_node_group_3]
instance_type = n2-standard-64
instance_capacity = 1
volume_size = 100
volume_type = pd-ssd

[storage]
data = /data

[metadata]
source = default_capella

[parameters]
CPU = n2-highmem-8 (8 vCPU)
Memory = 64 GB
Disk = pd-ssd, 300GB, 15000 IOPS