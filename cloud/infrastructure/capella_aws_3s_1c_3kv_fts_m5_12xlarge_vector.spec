[infrastructure]
provider = capella
backend = aws

[clusters]
couchbase1 =
    ec2.ec2_cluster_1.ec2_node_group_1.1:kv,fts
    ec2.ec2_cluster_1.ec2_node_group_1.2:kv,fts
    ec2.ec2_cluster_1.ec2_node_group_1.3:kv,fts

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
instance_type = m5.12xlarge
instance_capacity = 3
volume_size = 3000
iops = 16000

[ec2_node_group_2]
instance_type = c5.12xlarge
instance_capacity = 1
volume_size = 1000
iops = 3000

[storage]
data = var/cb/data

[metadata]
source = default_capella

[parameters]
cpu = 48vCPU
memory = 192GB
disk = GP3, 3000GB, 16000 IOPS
