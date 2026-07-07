[infrastructure]
provider = capella
backend = aws

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv
        ec2.ec2_cluster_1.ec2_node_group_2.1:index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_2.2:index,n1ql

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_3.1

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = gp3

[ec2_node_group_1]
instance_type = c5.2xlarge
instance_capacity = 3
volume_size = 100
iops = 8000

[ec2_node_group_2]
instance_type = m5.8xlarge
instance_capacity = 2
volume_size = 300
iops = 8000

[ec2_node_group_3]
instance_type = c5.9xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[metadata]
source = default_capella

[parameters]
OS = Amazon Linux 2
CPU = KV: c5.2xlarge (8 vCPU), Index/Query: m5.8xlarge (32 vCPU)
Memory = KV: 16GB, Index/Query: 128GB
Disk = EBS gp3
Note = Index service co-located on query nodes (Capella requires Index with Query); no GSI is built - external Iceberg collections have no local indexes, so the index service stays idle and query keeps full m5.8xlarge horsepower.
