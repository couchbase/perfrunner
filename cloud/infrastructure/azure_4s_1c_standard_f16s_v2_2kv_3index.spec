[infrastructure]
provider = azure
type = azurerm

[clusters]
couchbase1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.3:index
    azurerm.azurerm_cluster_1.azurerm_node_group_1.4:index
    azurerm.azurerm_cluster_1.azurerm_node_group_1.5:index

[clients]
workers1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_2.1

[utilities]
profile = default

[azurerm]
clusters = azurerm_cluster_1

[azurerm_cluster_1]
node_groups = azurerm_node_group_1,azurerm_node_group_2
storage_class = Premium_LRS

[azurerm_node_group_1]
instance_type = Standard_F16s_v2
instance_capacity = 5
volume_size = 512
disk_tier = P20
iops = 2300

[azurerm_node_group_2]
instance_type = Standard_D32as_v4
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[parameters]
os = Ubuntu 20
cpu = Standard_F16s_v2 (16 vCPU)
memory = 32 GB
disk = Premium SSD 512GB (P20)
