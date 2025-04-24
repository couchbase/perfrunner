[infrastructure]
provider = gcp
type = gce
os_arch = arm
service = columnar
cloud_storage = 1

[clusters]
datasource =
        gce.gce_cluster_1.gce_node_group_1.1:kv,n1ql
        gce.gce_cluster_1.gce_node_group_1.2:kv,n1ql
        gce.gce_cluster_1.gce_node_group_1.3:kv,n1ql
        gce.gce_cluster_1.gce_node_group_1.4:kv,n1ql

goldfish =
        gce.gce_cluster_2.gce_node_group_4.1:kv,cbas
        gce.gce_cluster_2.gce_node_group_4.2:kv,cbas
        gce.gce_cluster_2.gce_node_group_4.3:kv,cbas
        gce.gce_cluster_2.gce_node_group_4.4:kv,cbas

[clients]
workers1 =
        gce.gce_cluster_1.gce_node_group_2.1

[utilities]
profile = default

[gce]
clusters = gce_cluster_1,gce_cluster_2

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_2
storage_class = pd-ssd

[gce_cluster_2]
node_groups = gce_node_group_4
storage_class = hyperdisk-balanced

[gce_node_group_1]
instance_type = c4a-standard-16
instance_capacity = 4
volume_size = 400
storage_class = hyperdisk-balanced
iops = 12000
volume_throughput = 400

[gce_node_group_2]
instance_type = n2-standard-64
instance_capacity = 1
volume_size = 200

[gce_node_group_4]
instance_type = c4a-standard-8-lssd
instance_capacity = 4
volume_size = 0
local_nvmes = 2

[storage]
data = /data/data
analytics = /data/analytics

[parameters]
OS = Ubuntu 20.04
CPU = Data: c4a-standard-16 (16 vCPU), Analytics: c4a-standard-8-lssd (8 vCPU)
Memory = Data: 64GB, Analytics: 32GB
Disk = Data: pd-ssd 400GB, Analytics: 2 x 375GiB NVMe