[infrastructure]
provider = capella
backend = aws
capella_arch = serverless

[clusters]
serverless =
    ec2.ec2_cluster_1.ec2_node_group_1.1:kv
    ec2.ec2_cluster_1.ec2_node_group_1.2:kv
    ec2.ec2_cluster_1.ec2_node_group_1.3:kv
    ec2.ec2_cluster_1.ec2_node_group_2.1:index
    ec2.ec2_cluster_1.ec2_node_group_2.2:index
    ec2.ec2_cluster_1.ec2_node_group_2.3:n1ql

[clients]
workers1 =
    ec2.ec2_cluster_1.ec2_node_group_3.1

[utilities]
brokers1 =
    ec2.ec2_cluster_1.ec2_node_group_4.1

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3,ec2_node_group_4
storage_class = gp3

[ec2_node_group_1]
instance_type = c6gd.2xlarge
instance_capacity = 3
volume_size = 500

[ec2_node_group_2]
instance_type = c6gd.4xlarge
instance_capacity = 3
volume_size = 500

[ec2_node_group_3]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[ec2_node_group_4]
instance_type = t3a.large
instance_capacity = 1
volume_size = 100

[direct_nebula]
instance_type = c6gn.medium
override_count = 1

[data_api]
instance_type = c6gn.medium
override_count = 1

[storage]
data = /data

[credentials]
ssh = root:couchbase

[parameters]
OS = Amazon Linux 2
CPU = KV: c6gd.2xlarge (8 vCPU), Index+Query: c6gd.4xlarge (16 vCPU)
Memory = KV: 16 GB, Index+Query: 32GB
Disk = EBS 500 GB
