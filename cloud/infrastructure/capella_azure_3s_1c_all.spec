[infrastructure]
provider = capella
backend = azure

[clusters]
couchbase1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv,index,n1ql,fts
    azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv,index,n1ql,fts
    azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv,index,n1ql,fts

[clients]
workers1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_2.1

[utilities]
profile = default

[azurerm]
clusters = azurerm_cluster_1

[azurerm_cluster_1]
node_groups = azurerm_node_group_1,azurerm_node_group_2
storage_class = Premium_LRS

[azurerm_node_group_1]
instance_type = Standard_D4s_v5
instance_capacity = 3
volume_size = 512
disk_tier = P20
iops = 2300

[azurerm_node_group_2]
instance_type = Standard_D32as_v4
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[metadata]
source = default_capella

[parameters]
os = Ubuntu 20.04
cpu = D4s v3
memory = 16 GB
disk = Premium SSD 128GB (P60), 500 IOPS
