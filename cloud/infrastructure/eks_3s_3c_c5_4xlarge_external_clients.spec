[infrastructure]
provider = aws
type = kubernetes
external_client = true

[clusters]
couchbase1 =
        k8s.k8s_cluster_1.k8s_node_group_1.1:kv
        k8s.k8s_cluster_1.k8s_node_group_1.2:kv
        k8s.k8s_cluster_1.k8s_node_group_1.3:kv

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1
        ec2.ec2_cluster_1.ec2_node_group_1.2
        ec2.ec2_cluster_1.ec2_node_group_1.3

[utilities]
brokers1 = ec2.ec2_cluster_1.ec2_node_group_2

[k8s]
clusters = k8s_cluster_1

[ec2]
clusters = ec2_cluster_1

[k8s_cluster_1]
node_groups = k8s_node_group_1
version = 1.22
storage_class = gp2

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2
storage_class = gp2

[k8s_node_group_1]
instance_type = c5.4xlarge
instance_capacity = 3
volume_size = 100

[ec2_node_group_1]
instance_type = c5.4xlarge
instance_capacity = 3
volume_size = 100

[ec2_node_group_2]
instance_type = t3a.large
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = Ubuntu 20
CPU = c5.4xlarge (16 vCPU)
Memory = 32 GB
Disk = EBS 100GB
