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

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_4.1

[utilities]
brokers1 = ec2.ec2_cluster_1.ec2_node_group_5.1

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3,ec2_node_group_4,ec2_node_group_5
storage_class = gp3

[ec2_node_group_1]
instance_type = c6gd.2xlarge
instance_capacity = 3
volume_size = 400
volume_type = gp3

[ec2_node_group_2]
instance_type = c6g.2xlarge
instance_capacity = 2
volume_size = 50
volume_type = gp3

[ec2_node_group_3]
instance_type = c6gd.4xlarge
instance_capacity = 2
volume_size = 400
volume_type = gp3

[ec2_node_group_4]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 30

[ec2_node_group_5]
instance_type = t3a.large
instance_capacity = 1
volume_size = 30

[direct_nebula]
instance_type = c6gn.medium
override_count = 3

[data_api]
instance_type = c6gn.medium
override_count = 1

[storage]
data = /data

[credentials]
ssh = root:couchbase

[parameters]
OS = Amazon Linux 2
CPU = Data: c6gd.2xlarge (8 vCPU), Index: c6gd.4xlarge (16 vCPU), Query: c6g.2xlarge (8 vCPU)
Memory = Data & Query: 16GB, Index: 32GB
Disk = EBS 400 GB