[infrastructure]
provider = aws
type = ec2
os_arch = arm
service = columnar

[clusters]
datasource =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv
        ec2.ec2_cluster_1.ec2_node_group_1.4:kv

goldfish =
        ec2.ec2_cluster_2.ec2_node_group_3.1:kv,cbas
        ec2.ec2_cluster_2.ec2_node_group_3.2:kv,cbas
        ec2.ec2_cluster_2.ec2_node_group_3.3:kv,cbas
        ec2.ec2_cluster_2.ec2_node_group_3.4:kv,cbas

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1,ec2_cluster_2

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2
storage_class = gp3

[ec2_cluster_2]
node_groups = ec2_node_group_3
storage_class = gp3

[ec2_node_group_1]
instance_type = r7gd.2xlarge
instance_capacity = 4

[ec2_node_group_2]
instance_type = c5.12xlarge
instance_capacity = 1
volume_size = 100

[ec2_node_group_3]
instance_type = m7gd.2xlarge
instance_capacity = 4

[storage]
data = /data/data
analytics = /data/analytics

[parameters]
OS = Amazon Linux 2
CPU = Data: r7gd.2xlarge (8 vCPU), Analytics: m7gd.2xlarge (8 vCPU)
Memory = Data: 64GB, Analytics: 32GB
Disk = Data: Local NVMe SSD 474 GB, Analytics: Local NVMe SSD 474GB
