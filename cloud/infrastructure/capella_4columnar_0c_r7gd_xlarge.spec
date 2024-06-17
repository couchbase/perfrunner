[infrastructure]
provider = capella
backend = aws
service = goldfish

[clusters]
goldfish =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,cbas
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,cbas
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,cbas
        ec2.ec2_cluster_1.ec2_node_group_1.4:kv,cbas

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1
storage_class = gp3

[ec2_node_group_1]
instance_type = r7gd.xlarge

[storage]
data = /data/data
analytics = /data/analytics

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = Amazon Linux 2
CPU = r7gd.xlarge (4 vCPU)
Memory = 32GB
Disk = NVMe SSD 237 GB