[infrastructure]
provider = capella
backend = aws
capella_arch = serverless

[clusters]
serverless =
    ec2.ec2_cluster_1.ec2_node_group_1.1:kv
    ec2.ec2_cluster_1.ec2_node_group_1.2:kv
    ec2.ec2_cluster_1.ec2_node_group_1.3:kv
    ec2.ec2_cluster_1.ec2_node_group_1.4:fts
    ec2.ec2_cluster_1.ec2_node_group_1.5:fts

[clients]
workers1 =
    ec2.ec2_cluster_1.ec2_node_group_2.1

[utilities]
brokers1 =
    ec2.ec2_cluster_1.ec2_node_group_3.1

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = GP3

[ec2_node_group_1]
instance_type = c6gd.4xlarge
instance_capacity = 5
volume_size = 500
iops = 12000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100
iops = 3000

[ec2_node_group_3]
instance_type = t3a.large
instance_capacity = 1
volume_size = 100
iops = 3000

[direct_nebula]
instance_type = c6gn.large
override_count = 1

[data_api]
instance_type = c6gn.large
override_count = 1

[storage]
data = var/cb/data

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
os = Amazon Linux 2
cpu = 16vCPU
memory = 32GB
disk = GP3, 500GB, 12000 IOPS
