[infrastructure]
provider = capella
backend = gcp

[clusters]
couchbase1 =
	gce.gce_cluster_1.gce_node_group_1.1:kv
	gce.gce_cluster_1.gce_node_group_1.2:kv
	gce.gce_cluster_1.gce_node_group_1.3:kv
	gce.gce_cluster_1.gce_node_group_1.4:eventing
	gce.gce_cluster_1.gce_node_group_1.5:eventing

[clients]
workers1 =
	gce.gce_cluster_1.gce_node_group_2.1

[utilities]
brokers1 = gce.gce_cluster_1.gce_node_group_3.1

[gce]
clusters = gce_cluster_1

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_2,gce_node_group_3
storage_class = pd-ssd

[gce_node_group_1]
instance_type = n2-custom-16-32768
instance_capacity = 5
volume_size = 300
volume_type = pd-ssd

[gce_node_group_2]
instance_type = n2-standard-64
instance_capacity = 1
volume_size = 100
volume_type = pd-ssd

[gce_node_group_3]
instance_type = e2-standard-2
instance_capacity = 1

[storage]
data = /data

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
OS = Ubuntu 20.04
CPU = n2-custom-16-32768 (16 vCPU)
Memory = 32 GB
Disk = pd-ssd, 300GB, 15000 IOPS