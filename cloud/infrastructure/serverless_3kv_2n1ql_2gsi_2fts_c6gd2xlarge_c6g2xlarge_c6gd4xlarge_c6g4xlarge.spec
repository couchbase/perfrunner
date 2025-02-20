[infrastructure]
provider = capella
backend = aws
capella_arch = serverless

[clusters]
serverless =
    ec2.ec2_cluster_1.ec2_node_group_1.1:kv
    ec2.ec2_cluster_1.ec2_node_group_1.2:kv
    ec2.ec2_cluster_1.ec2_node_group_1.3:kv
    ec2.ec2_cluster_1.ec2_node_group_2.1:n1ql
    ec2.ec2_cluster_1.ec2_node_group_2.2:n1ql
    ec2.ec2_cluster_1.ec2_node_group_3.1:index
    ec2.ec2_cluster_1.ec2_node_group_3.2:index
    ec2.ec2_cluster_1.ec2_node_group_4.1:fts
    ec2.ec2_cluster_1.ec2_node_group_4.2:fts

[clients]
workers1 =
    ec2.ec2_cluster_1.ec2_node_group_5.1

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3,ec2_node_group_4,ec2_node_group_5
storage_class = gp3

[ec2_node_group_1]
instance_type = c6gd.2xlarge
instance_capacity = 3
volume_size = 500

[ec2_node_group_2]
instance_type = c6g.2xlarge
instance_capacity = 3
volume_size = 50

[ec2_node_group_3]
instance_type = c6gd.4xlarge
instance_capacity = 3
volume_size = 400

[ec2_node_group_4]
instance_type = c6g.4xlarge
instance_capacity = 3
volume_size = 400

[ec2_node_group_5]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[direct_nebula]
instance_type = c6gn.large
override_count = 1

[data_api]
instance_type = c6gn.large
override_count = 1

[storage]
data = /data

[credentials]
ssh = root:couchbase

[parameters]
OS = Amazon Linux 2
CPU = KV+Query: 8 vCPU, Index+FTS: 16 vCPU
Memory = KV+Query: 16 GB, Index+FTS: 32GB
Disk = EBS, GP3, 3000 IOPS, KV: 500GB, Query: 50GB, Index+FTS: 400GB
