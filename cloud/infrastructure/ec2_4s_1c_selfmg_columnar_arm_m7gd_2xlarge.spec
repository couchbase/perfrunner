[infrastructure]
provider = aws
type = ec2
os_arch = arm
service = columnar

[clusters]
goldfish =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,cbas
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,cbas
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,cbas
        ec2.ec2_cluster_1.ec2_node_group_1.4:kv,cbas

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
instance_type = m7gd.2xlarge
instance_capacity = 4

[ec2_node_group_2]
instance_type = r5.2xlarge
instance_capacity = 1

[storage]
data = /data/data
analytics = /data/analytics

[parameters]
OS = Ubuntu 20.04
CPU = m7gd.2xlarge (8 vCPU)
Memory = 32GB
Disk = Local NVMe SSD 474GB
