[infrastructure]
provider = capella
backend = aws

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv
        ec2.ec2_cluster_1.ec2_node_group_2.1:cbas
        ec2.ec2_cluster_1.ec2_node_group_2.2:cbas
        ec2.ec2_cluster_1.ec2_node_group_2.3:cbas

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_3.1

[utilities]
brokers1 = ec2.ec2_cluster_1.ec2_node_group_4.1

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3,ec2_node_group_4
storage_class = gp3

[ec2_node_group_1]
instance_type = c5.2xlarge
instance_capacity = 3
volume_size = 300
iops = 16000

[ec2_node_group_2]
instance_type = m5.2xlarge
instance_capacity = 3
volume_size = 300
iops = 16000

[ec2_node_group_3]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 300

[ec2_node_group_4]
instance_type = c5.9xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = var/cb/data

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
OS = Amazon Linux 2
CPU = Data: c5.2xlarge (8 vCPU), Analytics: m5.2xlarge (8 vCPU)
Memory = Data: 16GB, Analytics: 32GB
Disk = EBS gp3, 300GB, 16000 IOPS
