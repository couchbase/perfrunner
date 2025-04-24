[infrastructure]
provider = aws
type = ec2

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv
        ec2.ec2_cluster_1.ec2_node_group_1.4:eventing
        ec2.ec2_cluster_1.ec2_node_group_1.5:eventing
        ec2.ec2_cluster_1.ec2_node_group_1.6:index,n1ql

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
instance_type = m5.xlarge
instance_capacity = 6
volume_size = 5000
volume_type = io2
iops = 64000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[parameters]
OS = Amazon Linux 2
CPU = m5.xlarge (4 vCPU)
Memory = 16 GB
Disk = EBS 5000GB