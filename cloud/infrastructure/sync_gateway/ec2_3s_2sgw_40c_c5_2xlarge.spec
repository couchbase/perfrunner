[infrastructure]
provider = aws
type = ec2

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
        ec2.ec2_cluster_1.ec2_node_group_3.21
        ec2.ec2_cluster_1.ec2_node_group_3.22
        ec2.ec2_cluster_1.ec2_node_group_3.23
        ec2.ec2_cluster_1.ec2_node_group_3.24
        ec2.ec2_cluster_1.ec2_node_group_3.25
        ec2.ec2_cluster_1.ec2_node_group_3.26
        ec2.ec2_cluster_1.ec2_node_group_3.27
        ec2.ec2_cluster_1.ec2_node_group_3.28
        ec2.ec2_cluster_1.ec2_node_group_3.29
        ec2.ec2_cluster_1.ec2_node_group_3.30
        ec2.ec2_cluster_1.ec2_node_group_3.31
        ec2.ec2_cluster_1.ec2_node_group_3.32
        ec2.ec2_cluster_1.ec2_node_group_3.33
        ec2.ec2_cluster_1.ec2_node_group_3.34
        ec2.ec2_cluster_1.ec2_node_group_3.35
        ec2.ec2_cluster_1.ec2_node_group_3.36
        ec2.ec2_cluster_1.ec2_node_group_3.37
        ec2.ec2_cluster_1.ec2_node_group_3.38
        ec2.ec2_cluster_1.ec2_node_group_3.39
        ec2.ec2_cluster_1.ec2_node_group_3.40

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = gp2

[ec2_node_group_1]
instance_type = m5.4xlarge
instance_capacity = 3
volume_size = 1000

[ec2_node_group_2]
instance_type = c5.2xlarge
instance_capacity = 2
volume_type = gp3
volume_size = 10

[ec2_node_group_3]
instance_type = c5.12xlarge
instance_capacity = 40
volume_size = 1500
volume_type = gp3
iops = 10000

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = CentOS 7
CPU = Data: m5.4xlarge (16 vCPU), syncgateways: c5.2xlarge (8 vCPU)
Memory = Data: 64 GB, syncgateways: 16 GB
Disk = Data: EBS 1TB, syncgateways: 10 GB