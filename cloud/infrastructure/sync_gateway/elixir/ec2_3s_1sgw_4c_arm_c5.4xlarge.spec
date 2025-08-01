[infrastructure]
provider = aws
type = ec2
os_arch = arm
cloud_ini = cloud/infrastructure/cloud_aws.ini

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,index,n1ql

[syncgateways]
syncgateways1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_3.1
        ec2.ec2_cluster_1.ec2_node_group_3.2
        ec2.ec2_cluster_1.ec2_node_group_3.3
        ec2.ec2_cluster_1.ec2_node_group_3.4

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = gp3

[ec2_node_group_1]
instance_type = c6gd.2xlarge
instance_capacity = 3
volume_size = 3500
volume_type = gp3
volume_throughput = 1000
iops = 12000

[ec2_node_group_2]
instance_type = c5.4xlarge
instance_capacity = 1
volume_type = gp3
volume_size = 10

[ec2_node_group_3]
instance_type = c5.12xlarge
instance_capacity = 4
volume_size = 1000

[storage]
data = /data

[parameters]
OS = Amazon Linux 2
CPU = c6gd.2xlarge (8 vCPU), syncgateways: c5.4xlarge (16 vCPU)
Memory = 16 GB, syncgateways: 32 GB
Disk = EBS 3500 GB[test_case], syncgateways: 10 GB