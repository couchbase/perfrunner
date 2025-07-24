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
model_name = meta/llama3-70b-instruct
instance_type = p4de.24xlarge
instance_capacity = 1

[parameters]
cpu = 96vCPU
memory = 1152GB
gpu = 8, NVIDIA A100, 640GB
