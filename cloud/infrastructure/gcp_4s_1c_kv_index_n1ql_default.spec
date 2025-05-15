[infrastructure]
provider = capella
backend = gcp

[clusters]
couchbase1 =
    gce.gce_cluster_1.gce_node_group_1.1:kv,index,n1ql
    gce.gce_cluster_1.gce_node_group_1.2:kv,index,n1ql
    gce.gce_cluster_1.gce_node_group_1.3:kv,index,n1ql
    gce.gce_cluster_1.gce_node_group_1.4:kv,index,n1ql

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
instance_type = n2-standard-4
instance_capacity = 4
volume_size = 100
volume_type = pd-ssd

[gce_node_group_2]
instance_type = n2-standard-64
instance_capacity = 1
volume_size = 500
volume_type = pd-extreme

[storage]
data = /data
backup = gs://perftest-gcp-backup

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
OS = Ubuntu 20.04
CPU = 4vCPU
Memory = 16GB
Disk = pd-ssd 100Gb 15000 IOPS
