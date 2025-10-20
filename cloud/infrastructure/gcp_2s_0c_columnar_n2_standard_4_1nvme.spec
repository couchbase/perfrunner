[infrastructure]
provider = gcp
type = gce
service = columnar

[clusters]
goldfish =
        gce.gce_cluster_1.gce_node_group_1.1:kv,cbas
        gce.gce_cluster_1.gce_node_group_1.2:kv,cbas

[gce]
clusters = gce_cluster_1

[gce_cluster_1]
node_groups = gce_node_group_1

[gce_node_group_1]
instance_type = n2-standard-4
instance_capacity = 2
volume_size = 0
local_nvmes = 1

[storage]
data = /data/data
analytics = /data/analytics

[parameters]
OS = Ubuntu 20.04
CPU = n2-standard-4 (4 vCPU)
Memory = 16GB
Disk = 375GiB NVMe