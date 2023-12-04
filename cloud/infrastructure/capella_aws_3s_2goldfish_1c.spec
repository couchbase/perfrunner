[infrastructure]
cloud_provider = aws

[cb.capella.provisioned]
datasource =
        node_group_1.1:kv,index,n1ql
        node_group_1.2:kv,index,n1ql
        node_group_1.3:kv,index,n1ql

[cb.capella.columnar]
goldfish =
        node_group_4.1:kv,cbas
        node_group_4.2:kv,cbas

[clients]
workers1 = node_group_2.1

[utilities]
brokers1 = node_group_3.1

[node_group_1]
instance_type = m5.4xlarge
storage_type = gp3
storage_size = 2000
iops = 16000

[node_group_2]
instance_type = c5.24xlarge
storage_type = gp3
storage_size = 100

[node_group_3]
instance_type = c5.2xlarge
storage_type = gp3
storage_size = 100

[node_group_4]
instance_type = c7gd.4xlarge
storage_type = gp3
storage_size = 100

[storage]
data = /data/data
analytics = /data/analytics

[credentials]
rest = Administrator:Password123!
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = Amazon Linux 2
CPU = Data/Index/Query: m5.4xlarge (16 vCPU), Analytics: c7gd.4xlarge (16 vCPU)
Memory = Data/Index/Query: 64GB, Analytics: 32GB
Disk = Data/Index/Query: EBS 2000GB GP3 16000 IOPS, Analytics: NVMe SSD 950 GB