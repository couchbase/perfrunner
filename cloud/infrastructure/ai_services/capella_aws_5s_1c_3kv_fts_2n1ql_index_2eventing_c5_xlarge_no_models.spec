[infrastructure]
provider = capella
backend = aws

[clusters]
couchbase1 =
    ec2.ec2_cluster_1.ec2_node_group_1.1:kv,fts
    ec2.ec2_cluster_1.ec2_node_group_1.2:kv,fts
    ec2.ec2_cluster_1.ec2_node_group_1.3:kv,fts
    ec2.ec2_cluster_1.ec2_node_group_1.4:n1ql,index
    ec2.ec2_cluster_1.ec2_node_group_1.5:n1ql,index
    ec2.ec2_cluster_1.ec2_node_group_1.6:eventing
    ec2.ec2_cluster_1.ec2_node_group_1.7:eventing

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
instance_type = c5.xlarge
instance_capacity = 7
volume_size = 500
iops = 12000

[ec2_node_group_2]
instance_type = c5.xlarge
instance_capacity = 1
volume_size = 100
iops = 3000

[storage]
data = var/cb/data

[metadata]
source = default_capella

[parameters]
os = Amazon Linux 2
cpu = 4vCPU
memory = 8GB
disk = GP3, 500GB, 12000 IOPS
