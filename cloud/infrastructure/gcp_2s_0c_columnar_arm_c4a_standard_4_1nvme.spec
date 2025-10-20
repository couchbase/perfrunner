[infrastructure]
provider = gcp
type = gce
os_arch = arm
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
instance_type = c4a-standard-4-lssd
instance_capacity = 2
volume_size = 0
local_nvmes = 1

[storage]
data = /data/data
analytics = /data/analytics

[parameters]
OS = Ubuntu 20.04
CPU = c4a-standard-4-lssd (4 vCPU)
Memory = 16GB
Disk = 375GiB NVMe