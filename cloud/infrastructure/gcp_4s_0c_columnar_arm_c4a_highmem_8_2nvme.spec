[infrastructure]
provider = gcp
type = gce
os_arch = arm
service = columnar
cloud_storage = 1

[clusters]
goldfish =
        gce.gce_cluster_1.gce_node_group_1.1:kv,cbas
        gce.gce_cluster_1.gce_node_group_1.2:kv,cbas
        gce.gce_cluster_1.gce_node_group_1.3:kv,cbas
        gce.gce_cluster_1.gce_node_group_1.4:kv,cbas

[gce]
clusters = gce_cluster_1

[gce_cluster_1]
node_groups = gce_node_group_1

[gce_node_group_1]
instance_type = c4a-highmem-8-lssd
instance_capacity = 2
volume_size = 0
local_nvmes = 2

[storage]
data = /data/data
analytics = /data/analytics

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = Ubuntu 20.04
CPU = c4a-highmem-8-lssd (8 vCPU)
Memory = 64GB
Disk = 2 x 375GiB NVMe