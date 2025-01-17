[infrastructure]
provider = aws
type = ec2
os_arch = arm

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv
        ec2.ec2_cluster_1.ec2_node_group_1.4:index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.5:index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.6:index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.7:index,n1ql


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
instance_type = m6gd.8xlarge
instance_capacity = 7
volume_size = 2000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 800

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
OS = Amazon Linux 2
CPU = m6gd.8xlarge (32 vCPU)
Memory = 128 GB
Disk = EBS 2000GB