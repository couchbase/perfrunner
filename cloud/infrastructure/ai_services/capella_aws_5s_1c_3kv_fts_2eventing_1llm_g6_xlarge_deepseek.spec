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
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2
storage_class = GP3

[ec2_node_group_1]
instance_type = c5.xlarge
instance_capacity = 5
volume_size = 100
iops = 3000

[ec2_node_group_2]
instance_type = c5.xlarge
instance_capacity = 1
volume_size = 100
iops = 3000

[text-generation]
model_name = deepseek-ai/DeepSeek-R1-Distill-Llama-8B
instance_type = g6.xlarge
instance_capacity = 1

[storage]
data = var/cb/data

[metadata]
source = default_capella

[parameters]
os = Amazon Linux 2
cpu = 4vCPU
memory = 8GB
disk = GP3, 100GB, 3000 IOPS
