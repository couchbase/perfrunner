[infrastructure]
provider = capella
backend = gcp

[clusters]
couchbase1 =
    gce.gce_cluster_1.gce_node_group_1.1:kv
    gce.gce_cluster_1.gce_node_group_1.2:kv
    gce.gce_cluster_1.gce_node_group_1.3:kv
    gce.gce_cluster_1.gce_node_group_2.1:fts
    gce.gce_cluster_1.gce_node_group_2.2:fts
    gce.gce_cluster_1.gce_node_group_2.3:fts
    gce.gce_cluster_1.gce_node_group_2.4:fts

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
instance_type = n2-custom-16-32768
instance_capacity = 3
volume_size = 3000
volume_type = pd-ssd

[gce_node_group_2]
instance_type = n2-custom-16-32768
instance_capacity = 4
volume_size = 3000
volume_type = pd-ssd

[gce_node_group_3]
instance_type = n2-standard-64
instance_capacity = 1
volume_size = 1000
volume_type = pd-ssd

[storage]
data = /data
backup = gs://perftest-gcp-backup

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Data: n2-custom-16-32768 (16vCPU), FTS: n2-custom-16-32768 (16vCPU)
Memory = Data: 32GB, Index/Query: 32GB
Disk = pd-ssd, Data: 3TB 25000 IOPS, Index/Query: 3TB 25000 IOPS
