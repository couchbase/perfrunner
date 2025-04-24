[infrastructure]
provider = aws
type = ec2
os_arch = al2

[clusters]
couchbase1 =
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
storage_class = gp2

[ec2_node_group_1]
instance_type = m5.2xlarge
instance_capacity = 4
volume_size = 1000
volume_type = gp3
iops = 16000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data
analytics = /analytics

[parameters]
OS = Ubuntu 20.04
CPU = m5.2xlarge (8 vCPU)
Memory = 32 GB
Disk = EBS 1TB (gp3)