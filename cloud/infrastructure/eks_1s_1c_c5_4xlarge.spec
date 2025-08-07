[infrastructure]
provider = aws
type = kubernetes

[clusters]
couchbase1 =
        k8s.k8s_cluster_1.k8s_node_group_1.1:kv

[clients]
workers1 =
        k8s.k8s_cluster_1.k8s_node_group_2.1

[utilities]
profile = default

[k8s]
clusters = k8s_cluster_1
worker_cpu_limit = 2
worker_mem_limit = 4

[k8s_cluster_1]
node_groups = k8s_node_group_1,k8s_node_group_2
storage_class = gp2

[k8s_node_group_1]
instance_type = c5.4xlarge
instance_capacity = 1
volume_size = 100

[k8s_node_group_2]
instance_type = c5.xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[parameters]
CPU = c5.4xlarge (16 vCPU)
Memory = 32 GB
Disk = EBS 100GB
