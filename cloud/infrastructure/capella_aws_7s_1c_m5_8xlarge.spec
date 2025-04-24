[infrastructure]
provider = capella
backend = aws

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv
        ec2.ec2_cluster_1.ec2_node_group_1.4:index
        ec2.ec2_cluster_1.ec2_node_group_1.5:index
        ec2.ec2_cluster_1.ec2_node_group_1.6:n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.7:n1ql

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
instance_type = m5.8xlarge
instance_capacity = 7
volume_size = 1500
iops = 16000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 1000

[storage]
data = var/cb/data

[metadata]
source = default_capella

[parameters]
OS = Amazon Linux 2
CPU = Data/Index/N1ql: m5.8xlarge (32 vCPU)
Memory = Data/Index/N1ql: 128GB
Disk = EBS gp3, 1500GB, 16000 IOPS
