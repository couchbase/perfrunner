[infrastructure]
provider = capella
backend = aws
app_services = false

[clusters]
couchbase1 = 
	ec2.ec2_cluster_1.ec2_node_group_1.1:kv,index,n1ql
	ec2.ec2_cluster_1.ec2_node_group_1.2:kv,index,n1ql
	ec2.ec2_cluster_1.ec2_node_group_1.3:kv,index,n1ql

[syncgateways]
syncgateways1 =
    ec2.ec2_cluster_1.ec2_node_group_2.1
    ec2.ec2_cluster_1.ec2_node_group_2.2

[clients]
workers1 = 
	ec2.ec2_cluster_1.ec2_node_group_3.1
    ec2.ec2_cluster_1.ec2_node_group_3.2
    ec2.ec2_cluster_1.ec2_node_group_3.3
    ec2.ec2_cluster_1.ec2_node_group_3.4
    ec2.ec2_cluster_1.ec2_node_group_3.5
    ec2.ec2_cluster_1.ec2_node_group_3.6
    ec2.ec2_cluster_1.ec2_node_group_3.7
    ec2.ec2_cluster_1.ec2_node_group_3.8
    ec2.ec2_cluster_1.ec2_node_group_3.9
    ec2.ec2_cluster_1.ec2_node_group_3.10
    ec2.ec2_cluster_1.ec2_node_group_3.11
    ec2.ec2_cluster_1.ec2_node_group_3.12
    ec2.ec2_cluster_1.ec2_node_group_3.13
    ec2.ec2_cluster_1.ec2_node_group_3.14
    ec2.ec2_cluster_1.ec2_node_group_3.15
    ec2.ec2_cluster_1.ec2_node_group_3.16
    ec2.ec2_cluster_1.ec2_node_group_3.17
    ec2.ec2_cluster_1.ec2_node_group_3.18
    ec2.ec2_cluster_1.ec2_node_group_3.19
    ec2.ec2_cluster_1.ec2_node_group_3.20

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = gp3

[ec2_node_group_1]
instance_type = m5.4xlarge
instance_capacity = 3
volume_size = 100
iops = 16000

[ec2_node_group_2]
instance_type = c5.2xlarge
instance_capacity = 2
volume_size = 100
iops = 3000

[ec2_node_group_3]
instance_type = c5.12xlarge
instance_capacity = 20
volume_size = 100

[metadata]
source = default_capella

[parameters]
OS = Amazon Linux 2
CPU = c5.2xlarge (8 vCPU)
Memory = 16 GB
Disk = EBS gp3, 100GB, 16000 IOPS