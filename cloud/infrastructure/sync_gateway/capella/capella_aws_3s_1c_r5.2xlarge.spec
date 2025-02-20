[infrastructure]
provider = capella
backend = aws
app_services = false

[clusters]
couchbase1 =
    ec2.ec2_cluster_1.ec2_node_group_1.1:kv,index,n1ql
    ec2.ec2_cluster_1.ec2_node_group_1.2:kv,index,n1ql
    ec2.ec2_cluster_1.ec2_node_group_1.3:kv,index,n1ql

[syncgateways]
syncgateways1 =
    ec2.ec2_cluster_1.ec2_node_group_2.1
    ec2.ec2_cluster_1.ec2_node_group_2.2

[clients]
workers1 =
    ec2.ec2_cluster_1.ec2_node_group_3.1

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = GP3

[ec2_node_group_1]
instance_type = r5.2xlarge
instance_capacity = 3
volume_size = 500
iops = 3000

[ec2_node_group_2]
instance_type = c5.2xlarge
instance_capacity = 2
volume_size = 100
iops = 3000

[ec2_node_group_3]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100
iops = 3000

[storage]
data = var/cb/data

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
os = Amazon Linux 2
cpu = 8vCPU
memory = 64GB
disk = GP3, 500GB, 3000 IOPS
