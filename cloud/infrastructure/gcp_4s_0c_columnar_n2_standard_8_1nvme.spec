[infrastructure]
provider = gcp
type = gce
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
storage_class = pd-ssd

[gce_node_group_1]
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
CPU = n2-standard-8 (8 vCPU)
Memory = 32GB
Disk = 375GiB NVMe