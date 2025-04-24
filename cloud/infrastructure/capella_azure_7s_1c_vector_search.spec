[infrastructure]
provider = capella
backend = azure

[clusters]
couchbase1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.4:index
    azurerm.azurerm_cluster_1.azurerm_node_group_1.5:index
    azurerm.azurerm_cluster_1.azurerm_node_group_1.6:n1ql
    azurerm.azurerm_cluster_1.azurerm_node_group_1.7:n1ql

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
instance_type = Standard_D32s_v4
instance_capacity = 7
volume_size = 2048
disk_tier = P40
iops = 7500

[azurerm_node_group_2]
instance_type = Standard_D32as_v4
instance_capacity = 1
volume_size = 2000

[storage]
data = /data

[metadata]
source = default_capella

[parameters]
os = Ubuntu 20.04
cpu = 32vcpu
memory = 128 GB
disk = Premium SSD 2048GB (P40), 7500 IOPS