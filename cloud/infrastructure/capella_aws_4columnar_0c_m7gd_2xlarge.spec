[infrastructure]
provider = capella
backend = aws
service = columnar

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
instance_type = m7gd.2xlarge

[parameters]
OS = Amazon Linux 2
CPU = m7gd.2xlarge (8 vCPU)
Memory = 32GB
Disk = NVMe SSD 474 GB