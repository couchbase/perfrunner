[infrastructure]
provider = capella
backend = gcp
service = columnar

[clusters]
columnar =
        gce.gce_cluster_1.gce_node_group_1.1:kv,cbas
        gce.gce_cluster_1.gce_node_group_1.2:kv,cbas
        gce.gce_cluster_1.gce_node_group_1.3:kv,cbas
        gce.gce_cluster_1.gce_node_group_1.4:kv,cbas

[gce]
clusters = gce_cluster_1

[gce_cluster_1]
node_groups = gce_node_group_1
storage_class = hyperdisk-balanced

[gce_node_group_1]
instance_type = c4a-standard-8-lssd

[parameters]
OS = Ubuntu
CPU = c4a-standard-8-lssd (8 vCPU)
Memory = 32GB
Disk = NVMe SSD 2 x 375 GB
