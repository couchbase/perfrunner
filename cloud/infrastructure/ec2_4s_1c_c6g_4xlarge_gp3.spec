[infrastructure]
provider = aws
type = ec2
os_arch = arm

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv
        ec2.ec2_cluster_1.ec2_node_group_1.4:index

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
instance_type = c6gd.4xlarge
instance_capacity = 4
volume_size = 4000
volume_type = gp3
volume_throughput = 1000
iops = 16000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[parameters]
OS = Amazon Linux 2
CPU = c6gd.4xlarge (16 vCPU)
Memory = 32 GB
Disk = EBS 4TB (gp3)