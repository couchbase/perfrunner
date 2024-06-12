[infrastructure]
provider = capella
backend = azure

[clusters]
couchbase1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv,index,n1ql,fts
    azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv,index,n1ql,fts
    azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv,index,n1ql,fts
    azurerm.azurerm_cluster_1.azurerm_node_group_1.4:kv,index,n1ql,fts

[clients]
workers1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_2.1

[utilities]
brokers1 = azurerm.azurerm_cluster_1.azurerm_node_group_3.1

[azurerm]
clusters = azurerm_cluster_1

[azurerm_cluster_1]
node_groups = azurerm_node_group_1,azurerm_node_group_2,azurerm_node_group_3
storage_class = Premium_LRS

[azurerm_node_group_1]
instance_type = Standard_D8s_v5
instance_capacity = 4
volume_size = 512
disk_tier = P20
iops = 2300

[azurerm_node_group_2]
instance_type = Standard_D32as_v4
instance_capacity = 1
volume_size = 100

[azurerm_node_group_3]
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
cpu = 8vcpu
memory = 32 GB
disk = Premium SSD 128GB (P60), 500 IOPS
