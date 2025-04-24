[infrastructure]
provider = capella
backend = azure

[clusters]
couchbase1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.4:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_3.1:fts
    azurerm.azurerm_cluster_1.azurerm_node_group_3.2:fts
    azurerm.azurerm_cluster_1.azurerm_node_group_3.3:fts
    azurerm.azurerm_cluster_1.azurerm_node_group_3.4:fts

[clients]
workers1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_2.1

[utilities]
profile = default

[azurerm]
clusters = azurerm_cluster_1

[azurerm_cluster_1]
node_groups = azurerm_node_group_1,azurerm_node_group_2,azurerm_node_group_3
storage_class = Premium_LRS

[azurerm_node_group_1]
instance_type = Standard_F16s_v2
instance_capacity = 4
volume_size = 3000
disk_tier = P50
iops = 16000

[azurerm_node_group_2]
instance_type = Standard_F64s_v2
instance_capacity = 1
volume_size = 100

[azurerm_node_group_3]
instance_type = Standard_F16s_v2
instance_capacity = 4
volume_size = 1000
disk_tier = P30
iops = 16000

[storage]
data = /data

[metadata]
source = default_capella

[parameters]
cpu = Standard_F32s_v2 (16 vCPU)
memory = 32 GB
disk = Premium SSD 3000GB (P30) KV, 1000GB (P50) FTS, 16000 IOPS
