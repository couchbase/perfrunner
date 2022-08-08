[infrastructure]
provider = aws
type = ec2
os_arch = arm

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv
        ec2.ec2_cluster_1.ec2_node_group_2.4:index
        ec2.ec2_cluster_1.ec2_node_group_2.5:index
        ec2.ec2_cluster_1.ec2_node_group_2.6:n1ql
        ec2.ec2_cluster_1.ec2_node_group_2.7:n1ql

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_3.1

[utilities]
brokers1 = ec2.ec2_cluster_1.ec2_node_group_4.1

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3,ec2_node_group_4
storage_class = gp2

[ec2_node_group_1]
instance_type = c6gd.2xlarge
instance_capacity = 3
volume_size = 100
volume_type = gp3
iops = 12000

[ec2_node_group_2]
instance_type = c6gd.4xlarge
instance_capacity = 4
volume_size = 100
volume_type = gp3
iops = 12000

[ec2_node_group_3]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[ec2_node_group_4]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = Amazon Linux 2
CPU = Data: c6gd.2xlarge (8 vCPU), Query & Index: c6gd.4xlarge (16 vCPU)
Memory = Data: 16GB, & Query & Index: 32GB
Disk = EBS 100 GB