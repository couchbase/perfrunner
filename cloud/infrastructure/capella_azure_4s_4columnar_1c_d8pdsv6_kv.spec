[infrastructure]
provider = capella
backend = azure
service = columnar
provisioned_cluster = provisioned

[clusters]
provisioned =
        azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv
        azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv
        azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv
        azurerm.azurerm_cluster_1.azurerm_node_group_1.4:kv

goldfish =
        azurerm.azurerm_cluster_2.azurerm_node_group_2.1:kv,cbas
        azurerm.azurerm_cluster_2.azurerm_node_group_2.2:kv,cbas
        azurerm.azurerm_cluster_2.azurerm_node_group_2.3:kv,cbas
        azurerm.azurerm_cluster_2.azurerm_node_group_2.4:kv,cbas

[clients]
workers1 =
        azurerm.azurerm_cluster_1.azurerm_node_group_3.1

[utilities]
profile = default

[azurerm]
clusters = azurerm_cluster_1,azurerm_cluster_2

[azurerm_cluster_1]
node_groups = azurerm_node_group_1,azurerm_node_group_3
storage_class = Premium_LRS

[azurerm_cluster_2]
node_groups = azurerm_node_group_2
storage_class = Premium_LRS

[azurerm_node_group_1]
instance_type = Standard_D16s_v5
instance_capacity = 4
volume_size = 4100
disk_tier = P60
iops = 16000

[azurerm_node_group_2]
instance_type = Standard_D8pds_v6
instance_capacity = 4

[azurerm_node_group_3]
instance_type = Standard_F64s_v2
instance_capacity = 1
volume_size = 100

[metadata]
source = default_capella

[parameters]
OS = Ubuntu 24
CPU = KV: Standard_D16s_v5 (16 vCPU), Columnar: Standard_D8pds_v6 (8 vCPU)
Memory = KV: 64GB, Columnar: 32GB
Disk = KV: Premium LRS 4100GB (P60), Columnar: NVMe SSD 440GB
