[infrastructure]
provider = capella
backend = gcp

[clusters]
couchbase1 =
        gce.gce_cluster_1.gce_node_group_1.1:kv,index,n1ql
        gce.gce_cluster_1.gce_node_group_1.2:kv,index,n1ql
        gce.gce_cluster_1.gce_node_group_1.3:kv,index,n1ql

[syncgateways]
syncgateways1 = 
        gce.gce_cluster_1.gce_node_group_2.1
        gce.gce_cluster_1.gce_node_group_2.2

[clients]
workers1 =
        gce.gce_cluster_1.gce_node_group_3.1
        gce.gce_cluster_1.gce_node_group_3.2
        gce.gce_cluster_1.gce_node_group_3.3
        gce.gce_cluster_1.gce_node_group_3.4

[utilities]
brokers1 = gce.gce_cluster_1.gce_node_group_4.1

[gce]
clusters = gce_cluster_1

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_2,gce_node_group_3,gce_node_group_4
storage_class = pd-extreme

[gce_node_group_1]
instance_type = n2-standard-16
instance_capacity = 4
volume_size = 300
volume_type = pd-ssd

[gce_node_group_2]
instance_type = n2-standard-8
instance_capacity = 4
volume_size = 300
volume_type = pd-ssd

[gce_node_group_3]
instance_type = n2-standard-32
instance_capacity = 4
volume_size = 300
volume_type = pd-ssd

[gce_node_group_4]
instance_type = n2-standard-32
instance_capacity = 1

[storage]
data = /data
backup = gs://perftest-gcp-backup

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
OS = Ubuntu 20
CPU = Data/Index/Query: n2-standard-16 (16 vCPU), Syncgateway: n2-standard-8 (8 vCPU)
Memory = Data/Index/Query: 64GB, Syncgateway: 32 GB
Disk = pd-ssd
