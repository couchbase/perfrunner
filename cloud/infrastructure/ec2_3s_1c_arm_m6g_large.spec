[infrastructure]
provider = aws
type = ec2
os_arch = arm

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,index,n1ql,fts
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,index,n1ql,fts
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,index,n1ql,fts

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2
storage_class = gp2

[ec2_node_group_1]
instance_type = m6g.large
instance_capacity = 3
volume_size = 40

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[parameters]
OS = Amazon Linux 2
CPU = m6g.large (2 vCPU)
Memory = 8 GB
Disk = EBS 40GB