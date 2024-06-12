[infrastructure]
provider = capella
backend = azure

[clusters]
couchbase1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_2.1:cbas
    azurerm.azurerm_cluster_1.azurerm_node_group_2.2:cbas
    azurerm.azurerm_cluster_1.azurerm_node_group_2.3:cbas
    azurerm.azurerm_cluster_1.azurerm_node_group_2.4:cbas

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
instance_type = Standard_F8s_v2
instance_capacity = 3
volume_size = 4100
disk_tier = P60
iops = 16000

[azurerm_node_group_2]
instance_type = Standard_D8s_v5
instance_capacity = 4
volume_size = 4100
disk_tier = P60
iops = 16000

[azurerm_node_group_3]
instance_type = Standard_D32as_v4
instance_capacity = 1
volume_size = 256

[azurerm_node_group_4]
instance_type = Standard_B2as_v2
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
os = CentOS 7
CPU = Data: Standard_F8s_v2 (8 vCPU), Analytics: Standard_D8s_v5 (8 vCPU)
Memory = Data: 16GB, Analytics: 32GB
disk = Premium SSD 4100GB (P60)
