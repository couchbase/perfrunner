[infrastructure]
provider = capella
backend = azure

[clusters]
couchbase1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.4:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.5:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.6:kv
couchbase2 =
    azurerm.azurerm_cluster_2.azurerm_node_group_1.1:kv
    azurerm.azurerm_cluster_2.azurerm_node_group_1.2:kv
    azurerm.azurerm_cluster_2.azurerm_node_group_1.3:kv
    azurerm.azurerm_cluster_2.azurerm_node_group_1.4:kv
    azurerm.azurerm_cluster_2.azurerm_node_group_1.5:kv
    azurerm.azurerm_cluster_2.azurerm_node_group_1.6:kv

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
node_groups = azurerm_node_group_1
storage_class = Premium_LRS

[azurerm_node_group_1]
instance_type = Standard_E8s_v5
instance_capacity = 6
volume_size = 4100
disk_tier = P60
iops = 16000

[azurerm_node_group_3]
instance_type = Standard_F64s_v2
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[metadata]
source = default_capella

[parameters]
cpu = Standard_E8s_v5 (8 vCPU)
memory = 64 GB
disk = Premium SSD 4100GB (P60), 16000 IOPS