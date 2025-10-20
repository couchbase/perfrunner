[infrastructure]
provider = azure
type = azurerm
os_arch = arm
service = columnar

[clusters]
datasource =
        azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv
        azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv
        azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv
        azurerm.azurerm_cluster_1.azurerm_node_group_1.4:kv

goldfish =
        azurerm.azurerm_cluster_2.azurerm_node_group_3.1:kv,cbas
        azurerm.azurerm_cluster_2.azurerm_node_group_3.2:kv,cbas
        azurerm.azurerm_cluster_2.azurerm_node_group_3.3:kv,cbas
        azurerm.azurerm_cluster_2.azurerm_node_group_3.4:kv,cbas

[clients]
workers1 =
        azurerm.azurerm_cluster_1.azurerm_node_group_2.1

[utilities]
profile = default

[azurerm]
clusters = azurerm_cluster_1,azurerm_cluster_2

[azurerm_cluster_1]
node_groups = azurerm_node_group_1,azurerm_node_group_2
storage_class = Premium_LRS

[azurerm_cluster_2]
node_groups = azurerm_node_group_3
storage_class = Premium_LRS

[azurerm_node_group_1]
instance_type = Standard_E8pds_v6
instance_capacity = 4

[azurerm_node_group_2]
instance_type = Standard_D48as_v4
instance_capacity = 1
volume_size = 100

[azurerm_node_group_3]
instance_type = Standard_D8pds_v6
instance_capacity = 4

[storage]
data = /data/data
analytics = /data/analytics

[parameters]
OS = Ubuntu 24
CPU = Data: Standard_E8pds_v6 (8 vCPU), Analytics: Standard_D8pds_v6 (8 vCPU)
Memory = Data: 64GB, Analytics: 32GB
Disk = Data: Local NVMe SSD 440GB, Analytics: Local NVMe SSD 440GB
