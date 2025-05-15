[infrastructure]
provider = capella
backend = azure

[clusters]
couchbase1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.4:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_2.5:index,n1ql
    azurerm.azurerm_cluster_1.azurerm_node_group_2.6:index,n1ql
    azurerm.azurerm_cluster_1.azurerm_node_group_2.7:index,n1ql
    azurerm.azurerm_cluster_1.azurerm_node_group_2.8:index,n1ql

[clients]
workers1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_3.1

[utilities]
profile = default

[azurerm]
clusters = azurerm_cluster_1

[azurerm_cluster_1]
node_groups = azurerm_node_group_1,azurerm_node_group_2,azurerm_node_group_3
storage_class = Premium_LRS

[azurerm_node_group_1]
instance_type = Standard_E8s_v5
instance_capacity = 4
disk_tier = P60
volume_size = 1000
iops = 16000

[azurerm_node_group_2]
instance_type = Standard_E32s_v5
instance_capacity = 4
disk_tier = P60
volume_size = 2000
iops = 16000

[azurerm_node_group_3]
instance_type = Standard_F64s_v2
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
OS = Ubuntu 20.04
CPU = Data: Standard_E16s_v3 (16vCPU), Index/Query: Standard_E32s_v3 (32vCPU)
Memory = Data: 128GB, Index/Query: 256GB
Disk = pd-ssd, Data: 1TB 16000 IOPS, Index/Query: 1.5TB 16000 IOPS
