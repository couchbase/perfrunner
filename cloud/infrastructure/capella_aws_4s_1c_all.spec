[infrastructure]
provider = capella
backend = aws

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,index,n1ql,fts
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,index,n1ql,fts
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,index,n1ql,fts
        ec2.ec2_cluster_1.ec2_node_group_1.4:kv,index,n1ql,fts

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2
storage_class = gp3

[ec2_node_group_1]
instance_type = m5.xlarge
instance_capacity = 4
volume_size = 100
iops = 3000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 300

[storage]
data = /data

[metadata]
source = default_capella

[parameters]
OS = Amazon Linux 2
CPU = Data/Index/N1QL: c5.xlarge (4 vCPU)
Memory = Data: 16GB, N1QL/Index: 8GB
Disk = EBS gp3, 100GB, 3000 IOPS

