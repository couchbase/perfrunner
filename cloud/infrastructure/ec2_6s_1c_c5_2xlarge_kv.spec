[infrastructure]
provider = aws
type = ec2

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv
        ec2.ec2_cluster_1.ec2_node_group_1.4:kv
        ec2.ec2_cluster_1.ec2_node_group_1.5:kv
        ec2.ec2_cluster_1.ec2_node_group_1.6:kv

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
instance_type = c5.2xlarge
instance_capacity = 6
volume_size = 600
iops = 16000
volume_throughput = 1000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[parameters]
OS = Ubuntu 20.04
CPU = c5.2xlarge (8 vCPU)
Memory = 16 GB
Disk = EBS gp3, 600GB, 16000 IOPS
