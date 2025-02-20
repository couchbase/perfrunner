[infrastructure]
provider = gcp
type = gce

[clusters]
couchbase1 =
    gce.gce_cluster_1.gce_node_group_1.1:kv
    gce.gce_cluster_1.gce_node_group_1.2:kv
    gce.gce_cluster_1.gce_node_group_1.3:kv
    gce.gce_cluster_1.gce_node_group_2.1:index,n1ql

[clients]
workers1 =
    gce.gce_cluster_1.gce_node_group_3.1

[utilities]
profile = default

[gce]
clusters = gce_cluster_1

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_2,gce_node_group_3
storage_class = pd-ssd

[gce_node_group_1]
instance_type = n2-highmem-8
instance_capacity = 3
volume_size = 700
volume_type = pd-ssd

[gce_node_group_2]
instance_type = n2-highmem-32
instance_capacity = 1
volume_size = 1000
volume_type = pd-ssd

[gce_node_group_3]
instance_type = n2-standard-64
instance_capacity = 1
volume_size = 1000
volume_type = pd-extreme
iops = 20000

[storage]
data = /data
backup = gs://perftest-gcp-backup

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = 8vCPU
Memory = 64GB
Disk = pd-ssd 1TB 21000 IOPS
