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
    azurerm.azurerm_cluster_1.azurerm_node_group_3.5
    azurerm.azurerm_cluster_1.azurerm_node_group_3.6
    azurerm.azurerm_cluster_1.azurerm_node_group_3.7
    azurerm.azurerm_cluster_1.azurerm_node_group_3.8
    azurerm.azurerm_cluster_1.azurerm_node_group_3.9
    azurerm.azurerm_cluster_1.azurerm_node_group_3.10
    azurerm.azurerm_cluster_1.azurerm_node_group_3.11
    azurerm.azurerm_cluster_1.azurerm_node_group_3.12
    azurerm.azurerm_cluster_1.azurerm_node_group_3.13
    azurerm.azurerm_cluster_1.azurerm_node_group_3.14
    azurerm.azurerm_cluster_1.azurerm_node_group_3.15
    azurerm.azurerm_cluster_1.azurerm_node_group_3.16
    azurerm.azurerm_cluster_1.azurerm_node_group_3.17
    azurerm.azurerm_cluster_1.azurerm_node_group_3.18
    azurerm.azurerm_cluster_1.azurerm_node_group_3.19
    azurerm.azurerm_cluster_1.azurerm_node_group_3.20

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
instance_type = Standard_D8as_v4
instance_capacity = 4
volume_size = 250

[azurerm_node_group_3]
instance_type = Standard_D32as_v4
instance_capacity = 20
volume_size = 250

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
cpu = Data: D16as v4, syncgateways: D8as v4
memory = Data: 64 GB, syncgateways: 32 GB
disk = Data: Premium SSD 250GB, syncgateways: Premium SSD 250GB
