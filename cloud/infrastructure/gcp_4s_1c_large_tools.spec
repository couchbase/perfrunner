[infrastructure]
provider = gcp
type = gce
cloud_storage = 1

[clusters]
couchbase1 =
        gce.gce_cluster_1.gce_node_group_1.1:kv
        gce.gce_cluster_1.gce_node_group_1.2:kv
        gce.gce_cluster_1.gce_node_group_1.3:kv
        gce.gce_cluster_1.gce_node_group_1.4:kv

[clients]
workers1 =
        gce.gce_cluster_1.gce_node_group_2.1

[utilities]
profile = default

[gce]
clusters = gce_cluster_1

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_2
storage_class = pd-extreme

[gce_node_group_1]
instance_type = n2d-standard-16
instance_capacity = 4
volume_size = 1000
volume_type = pd-extreme
iops = 20000

[gce_node_group_2]
instance_type = n2-standard-32
instance_capacity = 1
volume_size = 1000
volume_type = pd-extreme
iops = 20000

[storage]
data = /data
backup = gs://perftest-gcp-backup

[parameters]
OS = Ubuntu 20.04
CPU = 16vCPU
Memory = 64GB
Disk = pd-extreme 1TB 20000 IOPS
