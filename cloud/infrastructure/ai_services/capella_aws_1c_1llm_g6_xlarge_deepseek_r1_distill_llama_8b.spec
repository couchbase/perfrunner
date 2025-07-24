[infrastructure]
provider = capella
backend = aws
model_services = true

[clients]
workers1 =
    ec2.ec2_cluster_1.ec2_node_group_1.1

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1
storage_class = GP3

[ec2_node_group_1]
instance_type = c5.xlarge
instance_capacity = 1
volume_size = 100
iops = 3000

[text-generation]
model_name = deepseek-ai/deepseek-r1-distill-llama-8b
instance_type = g6.xlarge
instance_capacity = 1

[parameters]
cpu = 4vCPU
memory = 16GB
gpu = 1, NVIDIA L4, 24GB
