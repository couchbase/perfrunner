[infrastructure]
provider = capella
backend = gcp

[clusters]
couchbase1 =
    gce.gce_cluster_1.gce_node_group_1.1:kv
    gce.gce_cluster_1.gce_node_group_1.2:kv
    gce.gce_cluster_1.gce_node_group_1.3:kv
    gce.gce_cluster_1.gce_node_group_1.4:index
    gce.gce_cluster_1.gce_node_group_1.5:index
    gce.gce_cluster_1.gce_node_group_1.6:n1ql
    gce.gce_cluster_1.gce_node_group_1.7:n1ql

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
instance_type = n2-standard-32
instance_capacity = 7
volume_size = 2000
volume_type = pd-ssd

[gce_node_group_2]
instance_type = n2-standard-64
instance_capacity = 1
volume_size = 2000
volume_type = pd-extreme

[storage]
data = /data
backup = gs://perftest-gcp-backup

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
OS = Ubuntu 20.04
CPU = 32vCPU
Memory = 128GB
Disk = pd-ssd 2000Gb
