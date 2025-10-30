[infrastructure]
provider = azure
type = azurerm
os_arch = arm
service = columnar
cloud_storage = 1

[clusters]
goldfish =
        azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv,cbas
        azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv,cbas
        azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv,cbas
        azurerm.azurerm_cluster_1.azurerm_node_group_1.4:kv,cbas

[azurerm]
clusters = azurerm_cluster_1

[azurerm_cluster_1]
node_groups = azurerm_node_group_1
storage_class = Premium_LRS

[azurerm_node_group_1]
instance_type = Standard_D8pds_v6
instance_capacity = 4

[storage]
data = /data/data
analytics = /data/analytics

[parameters]
OS = Ubuntu 24
CPU = Standard_D8pds_v6 (8 vCPU)
Memory = 32GB
Disk = Local NVMe SSD 440GB