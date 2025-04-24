[infrastructure]
provider = aws
type = ec2
os_arch = arm
service = columnar
cloud_storage = 1

[clusters]
goldfish =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,cbas
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,cbas

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
instance_type = c7gd.4xlarge
instance_capacity = 2
volume_size = 100

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data/data
analytics = /data/analytics

[parameters]
OS = Amazon Linux 2
CPU = c7gd.4xlarge (16 vCPU)
Memory = 32GB
Disk = NVMe SSD 950 GB