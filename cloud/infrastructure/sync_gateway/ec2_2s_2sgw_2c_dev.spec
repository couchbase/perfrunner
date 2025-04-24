[infrastructure]
provider = aws
type = ec2

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,index,n1ql

[syncgateways]
syncgateways1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1
        ec2.ec2_cluster_1.ec2_node_group_2.2

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_3.1
        ec2.ec2_cluster_1.ec2_node_group_3.2

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = gp2

[ec2_node_group_1]
instance_type = c5.24xlarge
instance_capacity = 2
volume_size = 100

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 2
volume_size = 100

[ec2_node_group_3]
instance_type = c5.2xlarge
instance_capacity = 2
volume_size = 100

[storage]
data = /data

[parameters]
OS = Ubuntu 20.04
CPU = c5.24xlarge (96 vCPU)
Memory = 192 GB
Disk = EBS 1TB