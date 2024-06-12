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

[utilities]
brokers1 = azurerm.azurerm_cluster_1.azurerm_node_group_4.1

[azurerm]
clusters = azurerm_cluster_1

[azurerm_cluster_1]
node_groups = azurerm_node_group_1,azurerm_node_group_2,azurerm_node_group_3,azurerm_node_group_4
storage_class = Premium_LRS

[azurerm_node_group_1]
instance_type = Standard_F16s_v2
instance_capacity = 3
volume_size = 250

[azurerm_node_group_2]
instance_type = Standard_F16s_v2
instance_capacity = 1
volume_size = 250

[azurerm_node_group_3]
instance_type = Standard_F16s_v2
instance_capacity = 1
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
cpu = Data: Standard_F16s_v2, syncgateways: Standard_F16s_v2
memory = Data: 32 GB, syncgateways: 32 GB
disk = Data: Premium SSD 4100GB (P60), 16000 IOPS, syncgateways: Premium SSD 4100GB (P60), 16000 IOPS
