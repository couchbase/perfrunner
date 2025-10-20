[infrastructure]
provider = aws
type = ec2
os_arch = arm
service = columnar

[clusters]
goldfish =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,cbas
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,cbas

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1
storage_class = gp3

[ec2_node_group_1]
instance_type = m7gd.xlarge
instance_capacity = 2
volume_size = 0

[storage]
data = /data/data
analytics = /data/analytics

[parameters]
OS = Ubuntu 20.04
CPU = m7gd.xlarge (4 vCPU)
Memory = 16GB
Disk = 474GB NVMe