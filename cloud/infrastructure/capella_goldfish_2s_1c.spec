[infrastructure]
provider = capella
backend = aws
service = goldfish

[clusters]
goldfish =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,cbas
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,cbas

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
instance_type = m7gd.4xlarge
instance_capacity = 2

[ec2_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[ec2_node_group_3]
instance_type = c5.9xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data/data
analytics = /data/analytics

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = Amazon Linux 2
CPU = m7gd.4xlarge (16 vCPU)
Memory = 64GB
Disk = NVMe SSD 950 GB