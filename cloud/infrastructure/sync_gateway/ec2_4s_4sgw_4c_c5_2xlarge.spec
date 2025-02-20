[infrastructure]
provider = aws
type = ec2
cloud_ini = cloud/infrastructure/cloud_aws.ini

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.4:kv,index,n1ql

[syncgateways]
syncgateways1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1
        ec2.ec2_cluster_1.ec2_node_group_2.2
        ec2.ec2_cluster_1.ec2_node_group_2.3
        ec2.ec2_cluster_1.ec2_node_group_2.4

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
storage_class = gp2

[ec2_node_group_1]
instance_type = c5.12xlarge
instance_capacity = 4
volume_size = 1000

[ec2_node_group_2]
instance_type = c5.2xlarge
instance_capacity = 4
volume_type = gp3
volume_size = 10

[ec2_node_group_3]
instance_type = c5.12xlarge
instance_capacity = 4
volume_size = 1000

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = CentOS 7
CPU = Data: c5.12xlarge (48 vCPU), syncgateways: c5.2xlarge (8 vCPU)
Memory = Data: 96 GB, syncgateways: 16 GB
Disk = Data: EBS 1TB, syncgateways: 10 GB