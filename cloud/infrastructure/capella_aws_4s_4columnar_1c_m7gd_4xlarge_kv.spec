[infrastructure]
provider = capella
backend = aws
service = columnar
provisioned_cluster = provisioned

[clusters]
provisioned =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv
        ec2.ec2_cluster_1.ec2_node_group_1.4:kv

goldfish =
        ec2.ec2_cluster_2.ec2_node_group_2.1:kv,cbas
        ec2.ec2_cluster_2.ec2_node_group_2.2:kv,cbas
        ec2.ec2_cluster_2.ec2_node_group_2.3:kv,cbas
        ec2.ec2_cluster_2.ec2_node_group_2.4:kv,cbas

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_3.1

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1,ec2_cluster_2

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_3
storage_class = gp3

[ec2_cluster_2]
node_groups = ec2_node_group_2
storage_class = gp3

[ec2_node_group_1]
instance_type = m5.4xlarge
volume_size = 1000
iops = 16000

[ec2_node_group_2]
instance_type = m7gd.4xlarge

[ec2_node_group_3]
instance_type = c5.24xlarge
volume_size = 100

[metadata]
source = default_capella

[parameters]
OS = Amazon Linux 2
CPU = KV: m5.4xlarge (16 vCPU), Columnar: m7gd.4xlarge (16 vCPU)
Memory = KV: 64GB, Columnar: 64GB
Disk = KV: 1000GB EBS GP3 16000 IOPS, Columnar: NVMe SSD 950 GB
