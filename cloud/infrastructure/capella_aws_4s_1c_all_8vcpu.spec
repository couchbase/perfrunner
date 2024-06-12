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
brokers1 = ec2.ec2_cluster_1.ec2_node_group_3.1

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = gp3

[ec2_node_group_1]
instance_type = m5.2xlarge
instance_capacity = 4
volume_size = 100
iops = 3000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 300

[ec2_node_group_3]
instance_type = t3a.large
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
OS = Amazon Linux 2
CPU = Data/Index/N1QL: c5.2xlarge (8 vCPU)
Memory = Data: 32GB, N1QL/Index: 32GB
Disk = EBS gp3, 100GB, 3000 IOPS

