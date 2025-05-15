[infrastructure]
provider = aws
type = ec2

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.4:kv,index,n1ql

[syncgateways]
syncgateways1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1

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
storage_class = gp2

[ec2_node_group_1]
instance_type = c5.12xlarge
instance_capacity = 4
volume_size = 1000

[ec2_node_group_2]
instance_type = m5.2xlarge
instance_capacity = 1
volume_size = 1000

[ec2_node_group_3]
instance_type = c5.2xlarge
instance_capacity = 20
volume_size = 1000

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = Ubuntu 20.04
CPU = m5.2xlarge (8 vCPU)
Memory = 32 GB
Disk = EBS 1TB