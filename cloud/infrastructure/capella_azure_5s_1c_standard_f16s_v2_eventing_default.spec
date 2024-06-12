[infrastructure]
provider = capella
backend = azure

[clusters]
couchbase1 =
    azurerm.azurerm_cluster_1.azurerm_node_group_1.1:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.2:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.3:kv
    azurerm.azurerm_cluster_1.azurerm_node_group_1.4:eventing
    azurerm.azurerm_cluster_1.azurerm_node_group_1.5:eventing

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
instance_type = Standard_F16s_v2
instance_capacity = 5
volume_size = 4100
disk_tier = P60
iops = 3000

[azurerm_node_group_2]
instance_type = Standard_F64s_v2
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
cpu = Standard_F32s_v2 (16 vCPU)
memory = 32 GB
disk = Premium SSD 4100GB (P60), 3000 IOPS