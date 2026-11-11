[infrastructure]
provider = capella
backend = gcp
backup_node_families = c3

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

[clients]
workers1 =
    gce.gce_cluster_1.gce_node_group_2.1

[utilities]
profile = default

[gce]
clusters = gce_cluster_1

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_2
storage_class = pd-ssd

[gce_node_group_1]
instance_type = n2-highmem-8
instance_capacity = 8
volume_size = 1000
volume_type = pd-ssd

[gce_node_group_2]
instance_type = n2-standard-64
instance_capacity = 1
volume_size = 100
volume_type = pd-ssd

[storage]
data = /data

[metadata]
source = default_capella

[parameters]
CPU = Data: n2-highmem-8 (8 vCPU)
Memory = Data: 64GB
Disk = pd-ssd, 1000GB
Backup = c3-standard-8