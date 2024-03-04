[infrastructure]
provider = aws
type = ec2
os_arch = arm
service = goldfish
cloud_storage = 1

[clusters]
datasource =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,n1ql

goldfish =
        ec2.ec2_cluster_2.ec2_node_group_4.1:kv,cbas

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1

[utilities]
brokers1 = ec2.ec2_cluster_1.ec2_node_group_3.1

[ec2]
clusters = ec2_cluster_1,ec2_cluster_2

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = gp3

[ec2_cluster_2]
node_groups = ec2_node_group_4
storage_class = gp3

[ec2_node_group_1]
instance_type = c6gd.2xlarge
instance_capacity = 2
volume_size = 0

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[ec2_node_group_3]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[ec2_node_group_4]
instance_type = m7gd.large
instance_capacity = 1
volume_size = 0

[storage]
data = /data/data
analytics = /data/analytics

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = Amazon Linux 2
CPU = Data: c6gd.2xlarge (8 vCPU), Analytics: m7gd.large (2 vCPU)
Memory = Data: 16GB, Analytics: 8GB
Disk = Data: NVMe SSD 474 GB, Analytics: NVMe SSD 118 GB