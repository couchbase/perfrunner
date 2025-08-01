[infrastructure]
provider = aws
type = kubernetes
os_arch = arm

[clusters]
couchbase1 =
        k8s.k8s_cluster_1.k8s_node_group_1.1:kv
        k8s.k8s_cluster_1.k8s_node_group_1.2:kv
        k8s.k8s_cluster_1.k8s_node_group_1.3:kv

[clients]
workers1 =
        k8s.k8s_cluster_1.k8s_node_group_2.1
        k8s.k8s_cluster_1.k8s_node_group_2.2
        k8s.k8s_cluster_1.k8s_node_group_2.3
        k8s.k8s_cluster_1.k8s_node_group_2.4
        k8s.k8s_cluster_1.k8s_node_group_2.5
        k8s.k8s_cluster_1.k8s_node_group_2.6

[utilities]
profile = default

[k8s]
clusters = k8s_cluster_1
worker_cpu_limit = 2
worker_mem_limit = 2

[k8s_cluster_1]
node_groups = k8s_node_group_1,k8s_node_group_2
version = 1.21
storage_class = gp2

[k8s_node_group_1]
instance_type = t4g.medium
instance_capacity = 3
volume_size = 100

[k8s_node_group_2]
instance_type = t3.medium
instance_capacity = 6
volume_size = 100

[storage]
data = /data

[parameters]
CPU = t4g.medium (64 vCPU)
Memory = 128 GB
Disk = EBS 1TB