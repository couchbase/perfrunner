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
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.4:kv,n1ql

goldfish =
        ec2.ec2_cluster_2.ec2_node_group_4.1:kv,cbas
        ec2.ec2_cluster_2.ec2_node_group_4.2:kv,cbas

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
instance_type = m6gd.4xlarge
instance_capacity = 4
volume_size = 0

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[ec2_node_group_3]
instance_type = c5.4xlarge
instance_capacity = 1
volume_size = 100

[ec2_node_group_4]
instance_type = c7gd.4xlarge
instance_capacity = 2
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
CPU = Data: m6gd.4xlarge (16 vCPU), Analytics: c7gd.4xlarge (16 vCPU)
Memory = Data: 64GB, Analytics: 32GB
Disk = Data, Analytics: NVMe SSD 950 GB