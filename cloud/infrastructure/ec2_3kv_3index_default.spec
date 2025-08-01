[infrastructure]
provider = aws
type = ec2

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv
        ec2.ec2_cluster_1.ec2_node_group_1.4:index
        ec2.ec2_cluster_1.ec2_node_group_1.5:index
        ec2.ec2_cluster_1.ec2_node_group_1.6:index

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2
storage_class = gp3

[ec2_node_group_1]
instance_type = m5.2xlarge
instance_capacity = 6
volume_size = 100
iops = 3000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 300

[storage]
data = /data

[parameters]
OS = Amazon Linux 2
CPU = Data/Index: c5.2xlarge (8 vCPU)
Memory = Data: 16GB, Index: 8GB
Disk = EBS gp3, 100GB, 3000 IOPS
