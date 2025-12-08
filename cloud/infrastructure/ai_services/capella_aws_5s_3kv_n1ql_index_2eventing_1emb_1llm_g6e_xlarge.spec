[infrastructure]
provider = capella
backend = aws
model_services = true

[clusters]
couchbase1 =
    ec2.ec2_cluster_1.ec2_node_group_1.1:kv,n1ql,index
    ec2.ec2_cluster_1.ec2_node_group_1.2:kv,n1ql,index
    ec2.ec2_cluster_1.ec2_node_group_1.3:kv,n1ql,index
    ec2.ec2_cluster_1.ec2_node_group_1.4:eventing
    ec2.ec2_cluster_1.ec2_node_group_1.5:eventing

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1
storage_class = GP3

[ec2_node_group_1]
instance_type = c5.xlarge
instance_capacity = 5
volume_size = 500
iops = 12000

[embedding-generation]
model_name = nvidia/llama-3.2-nv-embedqa-1b-v2
instance_type = g6e.xlarge
instance_capacity = 1

[text-generation]
model_name = mistralai/mistral-7b-instruct-v0.3
instance_type = g6e.xlarge
instance_capacity = 1

[metadata]
source = default_capella

[parameters]
OS = Amazon Linux 2
CPU = 4vCPU
Memory = 8GB
GPU = 1, NVIDIA L40S, 48GB
Disk = GP3, 500GB, 12000 IOPS
