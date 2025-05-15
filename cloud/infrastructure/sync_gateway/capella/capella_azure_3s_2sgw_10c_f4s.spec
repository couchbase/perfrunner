[infrastructure]
provider = capella
backend = azure

[clusters]
couchbase1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv,index,n1ql
    azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv,index,n1ql
    azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv,index,n1ql

[syncgateways]
syncgateways1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_2.1
    azurerm.azurerm_cluster_1.azurerm_node_group_2.2

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
volume_size = 4100
disk_tier = P60
iops = 16000

[azurerm_node_group_2]
instance_type = Standard_F4s_v2
instance_capacity = 2
volume_size = 4100
disk_tier = P60
iops = 16000

[azurerm_node_group_3]
instance_type = Standard_F16s_v2
instance_capacity = 20
volume_size = 100

[storage]
data = /data

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase

[parameters]
os = Ubuntu 20.04
cpu = Standard_F16s_v2 (16 vCPU)
memory = 32 GB
disk = Premium SSD 4100GB (P60), 16000 IOPS