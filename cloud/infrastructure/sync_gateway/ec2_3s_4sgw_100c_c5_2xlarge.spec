[infrastructure]
provider = aws
type = ec2

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,index,n1ql

[syncgateways]
syncgateways1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1
        ec2.ec2_cluster_1.ec2_node_group_2.2
        ec2.ec2_cluster_1.ec2_node_group_2.3
        ec2.ec2_cluster_1.ec2_node_group_2.4

[clients]
workers1 =
        ec2.ec2_cluster_1.ec2_node_group_3.1
        ec2.ec2_cluster_1.ec2_node_group_3.2
        ec2.ec2_cluster_1.ec2_node_group_3.3
        ec2.ec2_cluster_1.ec2_node_group_3.4
        ec2.ec2_cluster_1.ec2_node_group_3.5
        ec2.ec2_cluster_1.ec2_node_group_3.6
        ec2.ec2_cluster_1.ec2_node_group_3.7
        ec2.ec2_cluster_1.ec2_node_group_3.8
        ec2.ec2_cluster_1.ec2_node_group_3.9
        ec2.ec2_cluster_1.ec2_node_group_3.10
        ec2.ec2_cluster_1.ec2_node_group_3.11
        ec2.ec2_cluster_1.ec2_node_group_3.12
        ec2.ec2_cluster_1.ec2_node_group_3.13
        ec2.ec2_cluster_1.ec2_node_group_3.14
        ec2.ec2_cluster_1.ec2_node_group_3.15
        ec2.ec2_cluster_1.ec2_node_group_3.16
        ec2.ec2_cluster_1.ec2_node_group_3.17
        ec2.ec2_cluster_1.ec2_node_group_3.18
        ec2.ec2_cluster_1.ec2_node_group_3.19
        ec2.ec2_cluster_1.ec2_node_group_3.20
        ec2.ec2_cluster_1.ec2_node_group_3.21
        ec2.ec2_cluster_1.ec2_node_group_3.22
        ec2.ec2_cluster_1.ec2_node_group_3.23
        ec2.ec2_cluster_1.ec2_node_group_3.24
        ec2.ec2_cluster_1.ec2_node_group_3.25
        ec2.ec2_cluster_1.ec2_node_group_3.26
        ec2.ec2_cluster_1.ec2_node_group_3.27
        ec2.ec2_cluster_1.ec2_node_group_3.28
        ec2.ec2_cluster_1.ec2_node_group_3.29
        ec2.ec2_cluster_1.ec2_node_group_3.30
        ec2.ec2_cluster_1.ec2_node_group_3.31
        ec2.ec2_cluster_1.ec2_node_group_3.32
        ec2.ec2_cluster_1.ec2_node_group_3.33
        ec2.ec2_cluster_1.ec2_node_group_3.34
        ec2.ec2_cluster_1.ec2_node_group_3.35
        ec2.ec2_cluster_1.ec2_node_group_3.36
        ec2.ec2_cluster_1.ec2_node_group_3.37
        ec2.ec2_cluster_1.ec2_node_group_3.38
        ec2.ec2_cluster_1.ec2_node_group_3.39
        ec2.ec2_cluster_1.ec2_node_group_3.40
        ec2.ec2_cluster_1.ec2_node_group_3.41
        ec2.ec2_cluster_1.ec2_node_group_3.42
        ec2.ec2_cluster_1.ec2_node_group_3.43
        ec2.ec2_cluster_1.ec2_node_group_3.44
        ec2.ec2_cluster_1.ec2_node_group_3.45
        ec2.ec2_cluster_1.ec2_node_group_3.46
        ec2.ec2_cluster_1.ec2_node_group_3.47
        ec2.ec2_cluster_1.ec2_node_group_3.48
        ec2.ec2_cluster_1.ec2_node_group_3.49
        ec2.ec2_cluster_1.ec2_node_group_3.50
        ec2.ec2_cluster_1.ec2_node_group_3.51
        ec2.ec2_cluster_1.ec2_node_group_3.52
        ec2.ec2_cluster_1.ec2_node_group_3.53
        ec2.ec2_cluster_1.ec2_node_group_3.54
        ec2.ec2_cluster_1.ec2_node_group_3.55
        ec2.ec2_cluster_1.ec2_node_group_3.56
        ec2.ec2_cluster_1.ec2_node_group_3.57
        ec2.ec2_cluster_1.ec2_node_group_3.58
        ec2.ec2_cluster_1.ec2_node_group_3.59
        ec2.ec2_cluster_1.ec2_node_group_3.60
        ec2.ec2_cluster_1.ec2_node_group_3.61
        ec2.ec2_cluster_1.ec2_node_group_3.62
        ec2.ec2_cluster_1.ec2_node_group_3.63
        ec2.ec2_cluster_1.ec2_node_group_3.64
        ec2.ec2_cluster_1.ec2_node_group_3.65
        ec2.ec2_cluster_1.ec2_node_group_3.66
        ec2.ec2_cluster_1.ec2_node_group_3.67
        ec2.ec2_cluster_1.ec2_node_group_3.68
        ec2.ec2_cluster_1.ec2_node_group_3.69
        ec2.ec2_cluster_1.ec2_node_group_3.70
        ec2.ec2_cluster_1.ec2_node_group_3.71
        ec2.ec2_cluster_1.ec2_node_group_3.72
        ec2.ec2_cluster_1.ec2_node_group_3.73
        ec2.ec2_cluster_1.ec2_node_group_3.74
        ec2.ec2_cluster_1.ec2_node_group_3.75
        ec2.ec2_cluster_1.ec2_node_group_3.76
        ec2.ec2_cluster_1.ec2_node_group_3.77
        ec2.ec2_cluster_1.ec2_node_group_3.78
        ec2.ec2_cluster_1.ec2_node_group_3.79
        ec2.ec2_cluster_1.ec2_node_group_3.80
        ec2.ec2_cluster_1.ec2_node_group_3.81
        ec2.ec2_cluster_1.ec2_node_group_3.82
        ec2.ec2_cluster_1.ec2_node_group_3.83
        ec2.ec2_cluster_1.ec2_node_group_3.84
        ec2.ec2_cluster_1.ec2_node_group_3.85
        ec2.ec2_cluster_1.ec2_node_group_3.86
        ec2.ec2_cluster_1.ec2_node_group_3.87
        ec2.ec2_cluster_1.ec2_node_group_3.88
        ec2.ec2_cluster_1.ec2_node_group_3.89
        ec2.ec2_cluster_1.ec2_node_group_3.90
        ec2.ec2_cluster_1.ec2_node_group_3.91
        ec2.ec2_cluster_1.ec2_node_group_3.92
        ec2.ec2_cluster_1.ec2_node_group_3.93
        ec2.ec2_cluster_1.ec2_node_group_3.94
        ec2.ec2_cluster_1.ec2_node_group_3.95
        ec2.ec2_cluster_1.ec2_node_group_3.96
        ec2.ec2_cluster_1.ec2_node_group_3.97
        ec2.ec2_cluster_1.ec2_node_group_3.98
        ec2.ec2_cluster_1.ec2_node_group_3.99
        ec2.ec2_cluster_1.ec2_node_group_3.100

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = gp2

[ec2_node_group_1]
instance_type = m5.4xlarge
instance_capacity = 3
volume_size = 1000

[ec2_node_group_2]
instance_type = c5.2xlarge
instance_capacity = 4
volume_type = gp3
volume_size = 10

[ec2_node_group_3]
instance_type = c5.4xlarge
instance_capacity = 100
volume_size = 1500
volume_type = gp3
iops = 10000

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = Ubuntu 20.04
CPU = Data: m5.4xlarge (16 vCPU), syncgateways: c5.2xlarge (8 vCPU)
Memory = Data: 64 GB, syncgateways: 16 GB
Disk = Data: EBS 1TB, syncgateways: 10 GB