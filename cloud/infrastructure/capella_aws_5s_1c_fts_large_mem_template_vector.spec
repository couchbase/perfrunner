[infrastructure]
provider = capella
backend = aws

[clusters]
couchbase1 =
    ec2.ec2_cluster_1.ec2_node_group_1.1:kv
    ec2.ec2_cluster_1.ec2_node_group_1.2:kv
    ec2.ec2_cluster_1.ec2_node_group_1.3:kv
    ec2.ec2_cluster_1.ec2_node_group_4.1:fts
    ec2.ec2_cluster_1.ec2_node_group_4.2:fts

[clients]
workers1 =
    ec2.ec2_cluster_1.ec2_node_group_2.1

[utilities]
brokers1 =
    ec2.ec2_cluster_1.ec2_node_group_3.1

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = GP3

[ec2_node_group_1]
instance_type = m5.xlarge
instance_capacity = 3
volume_size = 500
iops = 9850

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100
iops = 3000

[ec2_node_group_3]
instance_type = c5.9xlarge
instance_capacity = 1
volume_size = 100
iops = 3000

[ec2_node_group_4]
instance_type = r5.2xlarge
instance_capacity = 2
volume_size = 500
iops = 9850

[storage]
data = var/cb/data

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
os = Amazon Linux 2
cpu = 4vCPU KV, 8vCPU FTS
memory = 32GB
disk = GP3, 500GB, 9850 IOPS
