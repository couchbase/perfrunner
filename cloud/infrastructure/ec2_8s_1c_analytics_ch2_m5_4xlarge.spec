[infrastructure]
provider = aws
type = ec2

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.4:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.5:cbas
        ec2.ec2_cluster_1.ec2_node_group_1.6:cbas
        ec2.ec2_cluster_1.ec2_node_group_1.7:cbas
        ec2.ec2_cluster_1.ec2_node_group_1.8:cbas

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
instance_type = m5.4xlarge
instance_capacity = 8
volume_size = 1000
volume_type = io2
iops = 37500

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data
analytics = /analytics

[parameters]
OS = Ubuntu 20.04
CPU = m5.4xlarge (16 vCPU)
Memory = 64 GB
Disk = EBS 1TB
