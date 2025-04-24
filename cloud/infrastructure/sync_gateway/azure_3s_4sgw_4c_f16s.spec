[infrastructure]
provider = azure
type = azurerm

[clusters]
couchbase1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv,index,n1ql
    azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv,index,n1ql
    azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv,index,n1ql

[syncgateways]
syncgateways1 = 
    azurerm.azurerm_cluster_1.azurerm_node_group_2.1
    azurerm.azurerm_cluster_1.azurerm_node_group_2.2
    azurerm.azurerm_cluster_1.azurerm_node_group_2.3
    azurerm.azurerm_cluster_1.azurerm_node_group_2.4

[clients]
workers1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_3.1
    azurerm.azurerm_cluster_1.azurerm_node_group_3.2
    azurerm.azurerm_cluster_1.azurerm_node_group_3.3
    azurerm.azurerm_cluster_1.azurerm_node_group_3.4

[utilities]
profile = default

[azurerm]
clusters = azurerm_cluster_1

[azurerm_cluster_1]
node_groups = azurerm_node_group_1,azurerm_node_group_2,azurerm_node_group_3
storage_class = Premium_LRS

[azurerm_node_group_1]
instance_type = Standard_F16s_v2
instance_capacity = 3
volume_size = 250

[azurerm_node_group_2]
instance_type = Standard_F16s_v2
instance_capacity = 4
volume_size = 250

[azurerm_node_group_3]
instance_type = Standard_F16s_v2
instance_capacity = 4
volume_size = 100

[storage]
data = /data

[parameters]
os = Ubuntu 20.04
cpu = Data: Standard_F16s_v2, syncgateways: Standard_F16s_v2
memory = Data: 32 GB, syncgateways: 32 GB
disk = Data: Premium SSD 4100GB (P60), 16000 IOPS, syncgateways: Premium SSD 4100GB (P60), 16000 IOPS