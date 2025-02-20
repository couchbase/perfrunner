[infrastructure]
provider = capella
backend = aws

[clusters]
couchbase1 =
    ec2.ec2_cluster_1.ec2_node_group_1.1:kv
    ec2.ec2_cluster_1.ec2_node_group_1.2:kv
    ec2.ec2_cluster_1.ec2_node_group_1.3:kv
    ec2.ec2_cluster_1.ec2_node_group_1.4:kv
    ec2.ec2_cluster_1.ec2_node_group_2.1:index,n1ql
    ec2.ec2_cluster_1.ec2_node_group_2.2:index,n1ql
    ec2.ec2_cluster_1.ec2_node_group_2.3:index,n1ql
    ec2.ec2_cluster_1.ec2_node_group_2.4:index,n1ql

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
instance_type = r5.2xlarge
instance_capacity = 4
volume_size = 1000
volume_type = gp3
iops = 16000

[ec2_node_group_2]
instance_type = r5.8xlarge
instance_capacity = 4
volume_size = 2000
volume_type = gp3
iops = 16000

[ec2_node_group_3]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 300

[storage]
data = /data

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = Data: r5.4xlarge (16vCPU), Index/Query: r5.8xlarge (32vCPU)
Memory = Data: 128GB, Index/Query: 256GB
Disk = pd-ssd, Data: 1TB 16000 IOPS, Index/Query: 1.5TB 16000 IOPS
