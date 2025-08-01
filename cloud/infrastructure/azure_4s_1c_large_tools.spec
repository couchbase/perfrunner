[infrastructure]
provider = azure
type = azurerm
cloud_storage = 1

[clusters]
couchbase1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.4:kv

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
instance_type = Standard_D16as_v4
instance_capacity = 4
volume_size = 250

[azurerm_node_group_2]
instance_type = Standard_D32as_v4
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[parameters]
os = Ubuntu 20.04
cpu = D16as v4
memory = 64 GB
disk = Premium SSD 250GB
