[infrastructure]
provider = aws
type = kubernetes
external_client = true

[clusters]
couchbase1 =
        k8s.k8s_cluster_1.k8s_node_group_1.1:kv

[k8s]
clusters = k8s_cluster_1

[k8s_cluster_1]
node_groups = k8s_node_group_1
storage_class = gp2

[k8s_node_group_1]
instance_type = c5.4xlarge
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
CPU = c5.4xlarge (16 vCPU)
Memory = 32 GB
Disk = EBS 100GB
