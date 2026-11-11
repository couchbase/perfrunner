[infrastructure]
provider = aws
type = ec2

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,fts
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,fts
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,fts

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
instance_type = m6a.8xlarge
instance_capacity = 3
volume_size = 2048

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 300

[storage]
data = /data

[parameters]
OS = Ubuntu 20.04
CPU = m6a.8xlarge (32 vCPU, AMD EPYC 7R13)
Memory = 128 GB
Disk = EBS 2048GB gp3
