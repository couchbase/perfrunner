[infrastructure]
provider = capella
backend = gcp

[clusters]
couchbase1 =
    gce.gce_cluster_1.gce_node_group_1.1:kv
    gce.gce_cluster_1.gce_node_group_1.2:kv
    gce.gce_cluster_1.gce_node_group_1.3:kv
    gce.gce_cluster_1.gce_node_group_2.1:index,n1ql
    gce.gce_cluster_1.gce_node_group_2.2:index,n1ql

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
instance_type = n2-custom-8-16384
instance_capacity = 3
volume_size = 300
volume_type = pd-ssd

[gce_node_group_2]
instance_type = n2-custom-16-32768
instance_capacity = 2
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
os = Ubuntu 20.04
cpu = Data: n2-custom-8-16384 (8 vCPU), Index/Query: n2-custom-16-32768 (16 vCPU)
memory = Data: 16GB, Index/Query: 32GB
Disk = pd-ssd, 300GB, 15000 IOPS