[infrastructure]
provider = capella
backend = aws
service = goldfish

[clusters]
goldfish =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,cbas
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,cbas

[storage]
data = /data/data
analytics = /data/analytics

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = Amazon Linux 2
CPU = c7gd.4xlarge (16 vCPU)
Memory = 32GB
Disk = NVMe SSD 950 GB