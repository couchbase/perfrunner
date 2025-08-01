[infrastructure]
provider = aws
type = kubernetes

[clusters]
couchbase1 =
        k8s.k8s_cluster_1.k8s_node_group_1.1:kv,n1ql,index
        k8s.k8s_cluster_1.k8s_node_group_1.2:kv,n1ql,index
        k8s.k8s_cluster_1.k8s_node_group_1.3:kv,n1ql,index

[clients]
workers1 =
        k8s.k8s_cluster_1.k8s_node_group_2.1
        k8s.k8s_cluster_1.k8s_node_group_2.2

[utilities]
profile = default

[syncgateways]
syncgateways1 =
        k8s.k8s_cluster_1.k8s_node_group_3.1
        k8s.k8s_cluster_1.k8s_node_group_3.2

[k8s]
clusters = k8s_cluster_1
worker_cpu_limit = 28
worker_mem_limit = 48

[k8s_cluster_1]
node_groups = k8s_node_group_1,k8s_node_group_2,k8s_node_group_3
version = 1.21
storage_class = gp2

[k8s_node_group_1]
instance_type = c5.9xlarge
instance_capacity = 3
volume_size = 100

[k8s_node_group_2]
instance_type = c5.9xlarge
instance_capacity = 2
volume_size = 100

[k8s_node_group_3]
instance_type = c5.9xlarge
instance_capacity = 2
volume_size = 100

[storage]
data = /data

[parameters]
CPU = c5.9xlarge (36 vCPU)
Memory = 72 GB
Disk = EBS 1TB
