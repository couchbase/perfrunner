[infrastructure]
provider = aws
type = ec2
os_arch = arm
service = columnar
cloud_storage = 1

[clusters]
goldfish =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,cbas
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,cbas

[kafka_clusters]
kafka =
        ec2.ec2_cluster_2.ec2_node_group_2.1:zk,broker
        ec2.ec2_cluster_2.ec2_node_group_2.2:broker

[ec2]
clusters = ec2_cluster_1,ec2_cluster_2

[ec2_cluster_1]
node_groups = ec2_node_group_1
storage_class = gp3

[ec2_cluster_2]
node_groups = ec2_node_group_2
storage_class = gp3

[ec2_node_group_1]
instance_type = c7gd.4xlarge
instance_capacity = 2
volume_size = 0

[ec2_node_group_2]
instance_type = m5.large
instance_capacity = 2
volume_size = 200

[storage]
data = /data/data
analytics = /data/analytics

[parameters]
OS = Amazon Linux 2
CPU = Analytics: c7gd.4xlarge (16 vCPU), Kafka: m5.large (2 vCPU)
Memory = Analytics: 32GB, Kafka: 8GB
Disk = Analytics: NVMe SSD 950 GB, Kafka: EBS GP3 200GB
