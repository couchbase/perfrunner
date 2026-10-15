[infrastructure]
provider = capella
backend = gcp

[clusters]
couchbase1 =
    gce.gce_cluster_1.gce_node_group_1.1:kv
    gce.gce_cluster_1.gce_node_group_1.2:kv
    gce.gce_cluster_1.gce_node_group_1.3:kv

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
instance_type = c2-standard-4
instance_capacity = 3
volume_size = 512
volume_type = pd-ssd

[gce_node_group_2]
instance_type = c2-standard-16
instance_capacity = 1
volume_size = 100
volume_type = pd-ssd

[storage]
data = /data

[metadata]
source = default_capella

[parameters]
CPU = Data: c2-standard-4 (4 vCPU)
Memory = Data: 16GB
Disk = pd-ssd, 512GB