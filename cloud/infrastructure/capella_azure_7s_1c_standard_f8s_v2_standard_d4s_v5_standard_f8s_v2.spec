[infrastructure]
provider = capella
backend = azure

[clusters]
couchbase1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_2.1:index
    azurerm.azurerm_cluster_1.azurerm_node_group_2.2:index
    azurerm.azurerm_cluster_1.azurerm_node_group_3.1:n1ql
    azurerm.azurerm_cluster_1.azurerm_node_group_3.2:n1ql

[clients]
workers1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_4.1

[utilities]
brokers1 = azurerm.azurerm_cluster_1.azurerm_node_group_5.1

[azurerm]
clusters = azurerm_cluster_1

[azurerm_cluster_1]
node_groups = azurerm_node_group_1,azurerm_node_group_2,azurerm_node_group_3,azurerm_node_group_4,azurerm_node_group_5
storage_class = Premium_LRS

[azurerm_node_group_1]
instance_type = Standard_F8s_v2
instance_capacity = 3
volume_size = 4100
disk_tier = P60
iops = 16000

[azurerm_node_group_2]
instance_type = Standard_D4s_v5
instance_capacity = 2
volume_size = 4100
disk_tier = P60
iops = 16000

[azurerm_node_group_3]
instance_type = Standard_F8s_v2
instance_capacity = 2
volume_size = 4100
disk_tier = P60
iops = 16000

[azurerm_node_group_4]
instance_type = Standard_D32as_v4
instance_capacity = 1
volume_size = 256

[azurerm_node_group_5]
instance_type = Standard_D16as_v4
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
os = Ubuntu 20
CPU = Data/Query: Standard_F8s_v2 (8 vCPU), Index: Standard_D4s_v5 (4 vCPU)
Memory = Data/Query: 16GB, Index: 16GB
disk = Premium SSD 4100GB (P60)
