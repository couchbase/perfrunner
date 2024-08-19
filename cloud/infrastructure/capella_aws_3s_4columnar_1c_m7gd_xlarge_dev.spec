[infrastructure]
provider = capella
backend = aws
service = columnar
provisioned_cluster = provisioned

[clusters]
provisioned =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,index,n1ql

goldfish =
        ec2.ec2_cluster_2.ec2_node_group_2.1:kv,cbas
        ec2.ec2_cluster_2.ec2_node_group_2.2:kv,cbas
        ec2.ec2_cluster_2.ec2_node_group_2.3:kv,cbas
        ec2.ec2_cluster_2.ec2_node_group_2.4:kv,cbas

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_3.1

[utilities]
brokers1 = ec2.ec2_cluster_1.ec2_node_group_4.1

[ec2]
clusters = ec2_cluster_1,ec2_cluster_2

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_3,ec2_node_group_4
storage_class = gp3

[ec2_cluster_2]
node_groups = ec2_node_group_2
storage_class = gp3

[ec2_node_group_1]
instance_type = m5.xlarge
volume_size = 50
iops = 3000

[ec2_node_group_2]
instance_type = m7gd.xlarge

[ec2_node_group_3]
instance_type = c5.9xlarge
volume_size = 100

[ec2_node_group_4]
instance_type = c5.xlarge
instance_capacity = 1
volume_size = 50

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
OS = Amazon Linux 2
CPU = KV+Index+N1QL: m5.xlarge (4 vCPU), Columnar: m7gd.xlarge (4 vCPU)
Memory = KV+Index+N1QL: 16GB, Columnar: 16GB
Disk = KV+Index+N1QL: 50GB EBS GP3 3000 IOPS, Columnar: NVMe SSD 237 GB