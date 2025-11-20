[infrastructure]
provider = capella
backend = aws

[clusters]
couchbase1 =
    ec2.ec2_cluster_1.ec2_node_group_1.1:kv
    ec2.ec2_cluster_1.ec2_node_group_1.2:kv
    ec2.ec2_cluster_1.ec2_node_group_1.3:kv
    ec2.ec2_cluster_1.ec2_node_group_1.4:eventing
    ec2.ec2_cluster_1.ec2_node_group_1.5:eventing

[clients]
workers1 =
    ec2.ec2_cluster_1.ec2_node_group_2.1

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2
storage_class = GP3

[ec2_node_group_1]
instance_type = c6g.4xlarge
instance_capacity = 5
volume_size = 500
iops = 12000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100
iops = 3000

[storage]
data = var/cb/data

[metadata]
source = default_capella

[parameters]
cpu = c6g.4xlarge (16 vCPU)
memory = 32GB
disk = GP3, 500GB, 12000 IOPS
