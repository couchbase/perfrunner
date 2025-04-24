[infrastructure]
provider = aws
type = kubernetes
os_arch = arm

[clusters]
couchbase1 =
        k8s.k8s_cluster_1.k8s_node_group_1.1:kv,n1ql,index
        k8s.k8s_cluster_1.k8s_node_group_1.2:kv,n1ql,index
        k8s.k8s_cluster_1.k8s_node_group_1.3:kv,n1ql,index
        k8s.k8s_cluster_1.k8s_node_group_1.4:kv,n1ql,index

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

[k8s_cluster_1]
node_groups = k8s_node_group_1,k8s_node_group_2
version = 1.21
storage_class = gp2

[k8s_node_group_1]
instance_type = c6g.16xlarge
instance_capacity = 4
volume_size = 100

[k8s_node_group_2]
instance_type = c5.24xlarge
instance_capacity = 6
volume_size = 100

[storage]
data = /data

[parameters]
CPU = c6g.16xlarge (96 vCPU)
Memory = 128 GB
Disk = EBS 1TB
