[infrastructure]
provider = aws
type = ec2
os_arch = al2

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_4.1:kv
        ec2.ec2_cluster_1.ec2_node_group_4.2:kv
        ec2.ec2_cluster_1.ec2_node_group_4.3:kv
        ec2.ec2_cluster_1.ec2_node_group_1.1:index
        ec2.ec2_cluster_1.ec2_node_group_1.2:n1ql

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1

[utilities]
brokers1 = ec2.ec2_cluster_1.ec2_node_group_3.1

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3,ec2_node_group_4
storage_class = gp2

[ec2_node_group_1]
instance_type = m5.4xlarge
instance_capacity = 2
volume_size = 1000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[ec2_node_group_3]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[ec2_node_group_4]
instance_type = m5.xlarge
instance_capacity = 3
volume_size = 1000

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = Amazon Linux 2
CPU = m5.xlarge (4 vCPU)
Memory = 16 GB
Disk = EBS 1000GB
