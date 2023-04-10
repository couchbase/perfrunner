[infrastructure]
provider = aws
type = ec2

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,n1ql,index
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,n1ql,index
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,n1ql,index

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
instance_type = i4i.xlarge
instance_capacity = 3
volume_size = 300
iops = 3000

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[ec2_node_group_3]
instance_type = c5.9xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = Amazon Linux 2
CPU = i4i.xlarge (4 vCPU)
Memory = 32 GB
Disk = NVMe, 937GB, 3000 IOPS
