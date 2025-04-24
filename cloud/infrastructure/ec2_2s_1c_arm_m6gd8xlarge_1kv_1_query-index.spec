[infrastructure]
provider = aws
type = ec2
os_arch = arm

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:index,n1ql

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = gp2

[ec2_node_group_1]
instance_type = m6gd.8xlarge
instance_capacity = 2
volume_size = 1000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 500

[ec2_node_group_3]
instance_type = c5.9xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[parameters]
OS = Amazon Linux 2
CPU = m6gd.8xlarge (32 vCPU)
Memory = 128 GB
Disk = EBS 300GB