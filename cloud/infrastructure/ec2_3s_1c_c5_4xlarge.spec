[infrastructure]
provider = aws
type = ec2

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1
        ec2.ec2_cluster_1.ec2_node_group_2.2
        ec2.ec2_cluster_1.ec2_node_group_2.3

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2
storage_class = gp2

[ec2_node_group_1]
instance_type = c5.4xlarge
instance_capacity = 3
volume_size = 3000
volume_type = io2
iops = 64000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 3
volume_size = 100

[storage]
data = /data

[parameters]
OS = Ubuntu 20.04
CPU = c5.4xlarge (16 vCPU)
Memory = 32 GB
Disk = EBS 3TB