[infrastructure]
provider = azure
backup = true
type = azurevm

[clusters]
couchbase1 =
    ec2.ec2_cluster_1.ec2_node_group_1.1:kv
    ec2.ec2_cluster_1.ec2_node_group_1.2:kv
    ec2.ec2_cluster_1.ec2_node_group_1.3:kv
    ec2.ec2_cluster_1.ec2_node_group_1.4:kv

[clients]
workers1 =
    ec2.ec2_cluster_1.ec2_node_group_2.1

[utilities]
brokers1 = ec2.ec2_cluster_1.ec2_node_group_3.1

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = Premium_LRS

[ec2_node_group_1]
instance_type = Standard_D16as_v4
instance_capacity = 4
volume_size = 250

[ec2_node_group_2]
instance_type = Standard_D32as_v4
instance_capacity = 1
volume_size = 100

[ec2_node_group_3]
instance_type = Standard_D16as_v4
instance_capacity = 1
volume_size = 100

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase

[parameters]
os = CentOS 7
cpu = D16as v4
memory = 64 GB
disk = Premium SSD 250GB
