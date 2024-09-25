[infrastructure]
provider = gcp
type = gce
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
brokers1 = gce.gce_cluster_1.gce_node_group_3.1

[gce]
clusters = gce_cluster_1,gce_cluster_2

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_2,gce_node_group_3
storage_class = pd-ssd

[gce_cluster_2]
node_groups = gce_node_group_4
storage_class = pd-ssd

[gce_node_group_1]
instance_type = n2-standard-16
instance_capacity = 4
volume_size = 100

[gce_node_group_2]
instance_type = n2-standard-64
instance_capacity = 1
volume_size = 200

[gce_node_group_3]
instance_type = n2-standard-4
instance_capacity = 1
volume_size = 0

[gce_node_group_4]
instance_type = n2-standard-8
instance_capacity = 4
volume_size = 0
local_nvmes = 1

[storage]
data = /data/data
analytics = /data/analytics

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = Ubuntu 20.04
CPU = Data: n2-standard-16 (16 vCPU), Analytics: n2-standard-8 (8 vCPU)
Memory = Data: 64GB, Analytics: 32GB
Disk = Data: pd-ssd 100GB, Analytics: 375GiB NVMe