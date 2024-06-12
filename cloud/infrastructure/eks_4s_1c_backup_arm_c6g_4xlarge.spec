[infrastructure]
provider = aws
type = kubernetes
os_arch = arm

[clusters]
couchbase1 =
        k8s.k8s_cluster_1.k8s_node_group_1.1:kv
        k8s.k8s_cluster_1.k8s_node_group_1.2:kv
        k8s.k8s_cluster_1.k8s_node_group_1.3:kv
        k8s.k8s_cluster_1.k8s_node_group_1.4:kv

[clients]
workers1 = k8s.k8s_cluster_1.k8s_node_group_2.1
backups1 = k8s.k8s_cluster_1.k8s_node_group_3.1

[utilities]
brokers1 = k8s.k8s_cluster_1.k8s_node_group_4.1
operators1 = k8s.k8s_cluster_1.k8s_node_group_4.2

[k8s]
clusters = k8s_cluster_1
worker_cpu_limit = 14
worker_mem_limit = 25

[k8s_cluster_1]
node_groups = k8s_node_group_1,k8s_node_group_2,k8s_node_group_3,k8s_node_group_4
version = 1.21
storage_class = gp2

[k8s_node_group_1]
instance_type = c6g.4xlarge
instance_capacity = 4
volume_size = 100

[k8s_node_group_2]
instance_type = c5.4xlarge
instance_capacity = 1
volume_size = 100

[k8s_node_group_3]
instance_type = c6g.4xlarge
instance_capacity = 1
volume_size = 100

[k8s_node_group_4]
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
OS = CentOS 7
CPU = c6g.4xlarge (16 vCPU)
Memory = 32 GB
Disk = EBS 1TB