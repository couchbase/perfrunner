[infrastructure]
provider = aws
type = ec2
os_arch = arm

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv:1
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv:1
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv:1
        ec2.ec2_cluster_1.ec2_node_group_2.1:n1ql:1
        ec2.ec2_cluster_1.ec2_node_group_2.2:n1ql:1
        ec2.ec2_cluster_1.ec2_node_group_3.1:index:1
        ec2.ec2_cluster_1.ec2_node_group_3.2:index:2

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

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = Amazon Linux 2
CPU = Data: c6gd.2xlarge (8 vCPU), Index: c6gd.4xlarge (16 vCPU), Query: c6g.2xlarge (8 vCPU)
Memory = Data & Query: 16GB, Index: 32GB
Disk = EBS 400 GB