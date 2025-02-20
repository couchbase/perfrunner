[infrastructure]
provider = aws
type = kubernetes

[clusters]
couchbase1 =
        k8s.k8s_cluster_1.k8s_node_group_1.1:kv
        k8s.k8s_cluster_1.k8s_node_group_1.2:kv
        k8s.k8s_cluster_1.k8s_node_group_1.3:kv
        k8s.k8s_cluster_1.k8s_node_group_1.4:kv
        k8s.k8s_cluster_1.k8s_node_group_1.5:kv
        k8s.k8s_cluster_1.k8s_node_group_1.6:kv
        k8s.k8s_cluster_1.k8s_node_group_1.7:index
        k8s.k8s_cluster_1.k8s_node_group_1.8:index
        k8s.k8s_cluster_1.k8s_node_group_1.9:index,n1ql
        k8s.k8s_cluster_1.k8s_node_group_1.10:index,n1ql
        k8s.k8s_cluster_1.k8s_node_group_1.11:empty

[clients]
workers1 =
        k8s.k8s_cluster_1.k8s_node_group_2.1
        k8s.k8s_cluster_1.k8s_node_group_2.2
        k8s.k8s_cluster_1.k8s_node_group_2.3
        k8s.k8s_cluster_1.k8s_node_group_2.4


[utilities]
profile = default

[k8s]
clusters = k8s_cluster_1
worker_cpu_limit = 14
worker_mem_limit = 25

[k8s_cluster_1]
node_groups = k8s_node_group_1,k8s_node_group_2
version = 1.21
storage_class = gp2

[k8s_node_group_1]
instance_type = c7g.4xlarge
instance_capacity = 11
volume_size = 100

[k8s_node_group_2]
instance_type = c5.9xlarge
instance_capacity = 4
volume_size = 100

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = Ubuntu 20
CPU = c7g.4xlarge (16 vCPU)
Memory = 32 GB
Disk = EBS 1TB
