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

[clients]
workers1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_3.1
    azurerm.azurerm_cluster_1.azurerm_node_group_3.2
    azurerm.azurerm_cluster_1.azurerm_node_group_3.3
    azurerm.azurerm_cluster_1.azurerm_node_group_3.4

[utilities]
brokers1 = azurerm.azurerm_cluster_1.azurerm_node_group_4.1

[azurerm]
clusters = azurerm_cluster_1

[azurerm_cluster_1]
node_groups = azurerm_node_group_1,azurerm_node_group_2,azurerm_node_group_3,azurerm_node_group_4
storage_class = Premium_LRS

[azurerm_node_group_1]
instance_type = Standard_D16as_v4
instance_capacity = 3
volume_size = 250

[azurerm_node_group_2]
instance_type = Standard_D16as_v4
instance_capacity = 1
volume_size = 250

[azurerm_node_group_3]
instance_type = Standard_D32as_v4
instance_capacity = 4
volume_size = 100

[azurerm_node_group_4]
instance_type = Standard_B2as_v2
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
os = CentOS 7
cpu = Data: D16as v4, syncgateways: D16as v4
memory = Data: 64 GB, syncgateways: 64 GB
disk = Data: Premium SSD 250GB, syncgateways: Premium SSD 250GB
