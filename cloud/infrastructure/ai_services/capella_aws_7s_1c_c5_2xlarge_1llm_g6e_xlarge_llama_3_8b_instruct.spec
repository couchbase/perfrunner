[infrastructure]
provider = capella
backend = aws
model_services = true

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv
        ec2.ec2_cluster_1.ec2_node_group_2.1:index
        ec2.ec2_cluster_1.ec2_node_group_2.2:index
        ec2.ec2_cluster_1.ec2_node_group_2.3:n1ql
        ec2.ec2_cluster_1.ec2_node_group_2.4:n1ql

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_3.1

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = gp3

[ec2_node_group_1]
instance_type = m5.2xlarge
instance_capacity = 3
volume_size = 1000
iops = 16000

[ec2_node_group_2]
instance_type = c5.2xlarge
instance_capacity = 4
volume_size = 1000
iops = 16000

[ec2_node_group_3]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 300

[storage]
data = var/cb/data

[metadata]
source = default_capella

[text-generation]
model_name = meta/llama3-8b-instruct
instance_type = g6e.xlarge
instance_capacity = 1

[parameters]
OS = Amazon Linux 2
CPU = Data: m5.2xlarge (8 vCPU), Index/Query: c5.2xlarge (8 vCPU)
Memory = Data: 32GB, Index/Query: 16GB
Disk = EBS gp3, 1000GB, 16000 IOPS
GPU = 1, NVIDIA L40S, 48GB
