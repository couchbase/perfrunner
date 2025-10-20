[infrastructure]
provider = gcp
type = gce
os_arch = arm
service = columnar

[clusters]
datasource =
        gce.gce_cluster_1.gce_node_group_1.1:kv
        gce.gce_cluster_1.gce_node_group_1.2:kv
        gce.gce_cluster_1.gce_node_group_1.3:kv
        gce.gce_cluster_1.gce_node_group_1.4:kv

goldfish =
        gce.gce_cluster_2.gce_node_group_3.1:kv,cbas
        gce.gce_cluster_2.gce_node_group_3.2:kv,cbas
        gce.gce_cluster_2.gce_node_group_3.3:kv,cbas
        gce.gce_cluster_2.gce_node_group_3.4:kv,cbas

[clients]
workers1 =
        gce.gce_cluster_1.gce_node_group_2.1

[utilities]
profile = default

[gce]
clusters = gce_cluster_1,gce_cluster_2

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_2
storage_class = hyperdisk-balanced

[gce_cluster_2]
node_groups = gce_node_group_3
storage_class = hyperdisk-balanced

[gce_node_group_1]
instance_type = c4a-highmem-8-lssd
instance_capacity = 4

[gce_node_group_2]
instance_type = n2-standard-48
instance_capacity = 1
storage_class = pd-ssd
volume_size = 100

[gce_node_group_3]
instance_type = c4a-standard-8-lssd
instance_capacity = 4

[storage]
data = /data/data
analytics = /data/analytics

[parameters]
OS = Ubuntu 24
CPU = Data: c4a-highmem-8-lssd (8 vCPU), Analytics: c4a-standard-8-lssd (8 vCPU)
Memory = Data: 64GB, Analytics: 32GB
Disk = Data: Local NVMe SSD 2 x 375GB, Analytics: Local NVMe SSD 2 x 375GB
