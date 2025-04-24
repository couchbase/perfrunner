[infrastructure]
provider = capella
backend = gcp
service = columnar
provisioned_cluster = provisioned

[clusters]
provisioned =
        gce.gce_cluster_1.gce_node_group_1.1:kv
        gce.gce_cluster_1.gce_node_group_1.2:kv
        gce.gce_cluster_1.gce_node_group_1.3:kv
        gce.gce_cluster_1.gce_node_group_1.4:kv

goldfish =
        gce.gce_cluster_2.gce_node_group_2.1:kv,cbas
        gce.gce_cluster_2.gce_node_group_2.2:kv,cbas
        gce.gce_cluster_2.gce_node_group_2.3:kv,cbas
        gce.gce_cluster_2.gce_node_group_2.4:kv,cbas

[clients]
workers1 =
        gce.gce_cluster_1.gce_node_group_3.1

[utilities]
profile = default

[gce]
clusters = gce_cluster_1,gce_cluster_2

[gce_cluster_1]
node_groups = gce_node_group_1,gce_node_group_3
storage_class = pd-ssd

[gce_cluster_2]
node_groups = gce_node_group_2
storage_class = hyperdisk-balanced

[gce_node_group_1]
instance_type = n2-standard-16
volume_size = 1000

[gce_node_group_2]
instance_type = c4a-standard-8-lssd

[gce_node_group_3]
instance_type = n2-standard-64
volume_size = 100

[metadata]
source = default_capella

[parameters]
OS = Ubuntu
CPU = KV: n2-standard-16 (16 vCPU), Columnar: c4a-standard-8-lssd (8 vCPU)
Memory = KV: 64GB, Columnar: 32GB
Disk = KV: 1000GB pd-ssd, Columnar: NVMe SSD 2 x 375 GB