[infrastructure]
provider = capella
backend = aws

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv
        ec2.ec2_cluster_1.ec2_node_group_2.1:index
        ec2.ec2_cluster_1.ec2_node_group_2.2:index
        ec2.ec2_cluster_1.ec2_node_group_3.3:n1ql
        ec2.ec2_cluster_1.ec2_node_group_3.4:n1ql
        ec2.ec2_cluster_1.ec2_node_group_2.3:index

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_4.1

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3,ec2_node_group_4
storage_class = gp3

[ec2_node_group_1]
instance_type = c5.2xlarge
instance_capacity = 3
volume_size = 300
iops = 16000

[ec2_node_group_2]
instance_type = m5.xlarge
instance_capacity = 3
volume_size = 300
iops = 8000

[ec2_node_group_3]
instance_type = c5.2xlarge
instance_capacity = 2
volume_size = 300
iops = 8000

[ec2_node_group_4]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 300
iops = 16000

[storage]
data = var/cb/data

[metadata]
source = default_capella

[parameters]
OS = Amazon Linux 2
CPU = Data/Query: c5.2xlarge (8 vCPU), Index: m5.xlarge (4 vCPU)
Memory = Data/Query: 16GB, Index: 16B
Disk = EBS gp3, 600GB, 16000 IOPS
