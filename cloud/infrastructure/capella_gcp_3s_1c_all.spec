[infrastructure]
provider = capella
backend = gcp

[clusters]
couchbase1 = 
	gce.gce_cluster_1.gce_node_group_1.1:kv,index,n1ql,fts
	gce.gce_cluster_1.gce_node_group_1.2:kv,index,n1ql,fts
	gce.gce_cluster_1.gce_node_group_1.3:kv,index,n1ql,fts

[clients]
workers1 = 
	gce.gce_cluster_1.gce_node_group_3.1

[utilities]
brokers1 = gce.gce_cluster_1.gce_node_group_3.1

[gce]
clusters = gce_cluster_1

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_2,gce_node_group_3
storage_class = pd-ssd

[gce_node_group_1]
instance_type = n2-standard-4
instance_capacity = 3
volume_size = 100
volume_type = pd-ssd

[gce_node_group_2]
instance_type = n2-standard-64
instance_capacity = 1
volume_size = 500
volume_type = pd-extreme

[gce_node_group_3]
instance_type = e2-standard-2
instance_capacity = 1

[storage]
data = /data
backup = gs://perftest-gcp-backup

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
OS = CentOS 7
CPU = 4vCPU
Memory = 16GB
Disk = pd-ssd 100Gb 15000 IOPS
