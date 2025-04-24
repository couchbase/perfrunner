[infrastructure]
provider = aws
type = kubernetes

[clusters]
couchbase1 =
        k8s.k8s_cluster_1.k8s_node_group_1.1:kv
        k8s.k8s_cluster_1.k8s_node_group_1.2:kv

[clients]
workers1 =
        k8s.k8s_cluster_1.k8s_node_group_2.1

[utilities]
profile = default

[k8s]
clusters = k8s_cluster_1

[k8s_cluster_1]
node_groups = k8s_node_group_1,k8s_node_group_2
version = 1.21
storage_class = gp2

[k8s_node_group_1]
instance_type = c5.24xlarge
instance_capacity = 2
volume_size = 100

[k8s_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[parameters]
CPU = c5.24xlarge (96 vCPU)
Memory = 192 GB
Disk = EBS 1TB