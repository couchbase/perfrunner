[infrastructure]
provider = capella
backend = aws
model_services = true

[clusters]
couchbase1 =
    ec2.ec2_cluster_1.ec2_node_group_1.1:kv,fts
    ec2.ec2_cluster_1.ec2_node_group_1.2:kv,fts
    ec2.ec2_cluster_1.ec2_node_group_1.3:kv,fts
    ec2.ec2_cluster_1.ec2_node_group_1.4:eventing
    ec2.ec2_cluster_1.ec2_node_group_1.5:eventing

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
instance_type = c5.xlarge
instance_capacity = 5
volume_size = 500
iops = 12000

[ec2_node_group_2]
instance_type = c5.xlarge
instance_capacity = 1
volume_size = 100
iops = 3000

[ec2_node_group_3]
instance_type = t3a.large
instance_capacity = 1
volume_size = 100
iops = 3000

[embedding-generation]
model_name = meta-llama/Llama-3.1-8B-Instruct
instance_type = g6.xlarge
instance_capacity = 1

[storage]
data = var/cb/data

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
os = Amazon Linux 2
cpu = 4vCPU
memory = 8GB
disk = GP3, 500GB, 12000 IOPS
