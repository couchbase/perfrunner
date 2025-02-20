[infrastructure]
provider = aws
type = ec2

[clusters]
couchbase1 =
        ec2.ec2_cluster_1.ec2_node_group_1.1:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.2:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.3:kv,index,n1ql
        ec2.ec2_cluster_1.ec2_node_group_1.4:kv,index,n1ql

[syncgateways]
syncgateways1 =
        ec2.ec2_cluster_1.ec2_node_group_2.1

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
        ec2.ec2_cluster_1.ec2_node_group_3.101
        ec2.ec2_cluster_1.ec2_node_group_3.102
        ec2.ec2_cluster_1.ec2_node_group_3.103
        ec2.ec2_cluster_1.ec2_node_group_3.104
        ec2.ec2_cluster_1.ec2_node_group_3.105
        ec2.ec2_cluster_1.ec2_node_group_3.106
        ec2.ec2_cluster_1.ec2_node_group_3.107
        ec2.ec2_cluster_1.ec2_node_group_3.108
        ec2.ec2_cluster_1.ec2_node_group_3.109
        ec2.ec2_cluster_1.ec2_node_group_3.110
        ec2.ec2_cluster_1.ec2_node_group_3.111
        ec2.ec2_cluster_1.ec2_node_group_3.112
        ec2.ec2_cluster_1.ec2_node_group_3.113
        ec2.ec2_cluster_1.ec2_node_group_3.114
        ec2.ec2_cluster_1.ec2_node_group_3.115
        ec2.ec2_cluster_1.ec2_node_group_3.116
        ec2.ec2_cluster_1.ec2_node_group_3.117
        ec2.ec2_cluster_1.ec2_node_group_3.118
        ec2.ec2_cluster_1.ec2_node_group_3.119
        ec2.ec2_cluster_1.ec2_node_group_3.120
        ec2.ec2_cluster_1.ec2_node_group_3.121
        ec2.ec2_cluster_1.ec2_node_group_3.122
        ec2.ec2_cluster_1.ec2_node_group_3.123
        ec2.ec2_cluster_1.ec2_node_group_3.124
        ec2.ec2_cluster_1.ec2_node_group_3.125
        ec2.ec2_cluster_1.ec2_node_group_3.126
        ec2.ec2_cluster_1.ec2_node_group_3.127
        ec2.ec2_cluster_1.ec2_node_group_3.128
        ec2.ec2_cluster_1.ec2_node_group_3.129
        ec2.ec2_cluster_1.ec2_node_group_3.130
        ec2.ec2_cluster_1.ec2_node_group_3.131
        ec2.ec2_cluster_1.ec2_node_group_3.132
        ec2.ec2_cluster_1.ec2_node_group_3.133
        ec2.ec2_cluster_1.ec2_node_group_3.134
        ec2.ec2_cluster_1.ec2_node_group_3.135
        ec2.ec2_cluster_1.ec2_node_group_3.136
        ec2.ec2_cluster_1.ec2_node_group_3.137
        ec2.ec2_cluster_1.ec2_node_group_3.138
        ec2.ec2_cluster_1.ec2_node_group_3.139
        ec2.ec2_cluster_1.ec2_node_group_3.140
        ec2.ec2_cluster_1.ec2_node_group_3.141
        ec2.ec2_cluster_1.ec2_node_group_3.142
        ec2.ec2_cluster_1.ec2_node_group_3.143
        ec2.ec2_cluster_1.ec2_node_group_3.144
        ec2.ec2_cluster_1.ec2_node_group_3.145
        ec2.ec2_cluster_1.ec2_node_group_3.146
        ec2.ec2_cluster_1.ec2_node_group_3.147
        ec2.ec2_cluster_1.ec2_node_group_3.148
        ec2.ec2_cluster_1.ec2_node_group_3.149
        ec2.ec2_cluster_1.ec2_node_group_3.150
        ec2.ec2_cluster_1.ec2_node_group_3.151
        ec2.ec2_cluster_1.ec2_node_group_3.152
        ec2.ec2_cluster_1.ec2_node_group_3.153
        ec2.ec2_cluster_1.ec2_node_group_3.154
        ec2.ec2_cluster_1.ec2_node_group_3.155
        ec2.ec2_cluster_1.ec2_node_group_3.156
        ec2.ec2_cluster_1.ec2_node_group_3.157
        ec2.ec2_cluster_1.ec2_node_group_3.158
        ec2.ec2_cluster_1.ec2_node_group_3.159
        ec2.ec2_cluster_1.ec2_node_group_3.160
        ec2.ec2_cluster_1.ec2_node_group_3.161
        ec2.ec2_cluster_1.ec2_node_group_3.162
        ec2.ec2_cluster_1.ec2_node_group_3.163
        ec2.ec2_cluster_1.ec2_node_group_3.164
        ec2.ec2_cluster_1.ec2_node_group_3.165
        ec2.ec2_cluster_1.ec2_node_group_3.166
        ec2.ec2_cluster_1.ec2_node_group_3.167
        ec2.ec2_cluster_1.ec2_node_group_3.168
        ec2.ec2_cluster_1.ec2_node_group_3.169
        ec2.ec2_cluster_1.ec2_node_group_3.170
        ec2.ec2_cluster_1.ec2_node_group_3.171
        ec2.ec2_cluster_1.ec2_node_group_3.172
        ec2.ec2_cluster_1.ec2_node_group_3.173
        ec2.ec2_cluster_1.ec2_node_group_3.174
        ec2.ec2_cluster_1.ec2_node_group_3.175
        ec2.ec2_cluster_1.ec2_node_group_3.176
        ec2.ec2_cluster_1.ec2_node_group_3.177
        ec2.ec2_cluster_1.ec2_node_group_3.178
        ec2.ec2_cluster_1.ec2_node_group_3.179
        ec2.ec2_cluster_1.ec2_node_group_3.180
        ec2.ec2_cluster_1.ec2_node_group_3.181
        ec2.ec2_cluster_1.ec2_node_group_3.182
        ec2.ec2_cluster_1.ec2_node_group_3.183
        ec2.ec2_cluster_1.ec2_node_group_3.184
        ec2.ec2_cluster_1.ec2_node_group_3.185
        ec2.ec2_cluster_1.ec2_node_group_3.186
        ec2.ec2_cluster_1.ec2_node_group_3.187
        ec2.ec2_cluster_1.ec2_node_group_3.188
        ec2.ec2_cluster_1.ec2_node_group_3.189
        ec2.ec2_cluster_1.ec2_node_group_3.190
        ec2.ec2_cluster_1.ec2_node_group_3.191
        ec2.ec2_cluster_1.ec2_node_group_3.192
        ec2.ec2_cluster_1.ec2_node_group_3.193
        ec2.ec2_cluster_1.ec2_node_group_3.194
        ec2.ec2_cluster_1.ec2_node_group_3.195
        ec2.ec2_cluster_1.ec2_node_group_3.196
        ec2.ec2_cluster_1.ec2_node_group_3.197
        ec2.ec2_cluster_1.ec2_node_group_3.198
        ec2.ec2_cluster_1.ec2_node_group_3.199
        ec2.ec2_cluster_1.ec2_node_group_3.200

[utilities]
profile = default

[ec2]
clusters = ec2_cluster_1

[ec2_cluster_1]
node_groups = ec2_node_group_1,ec2_node_group_2,ec2_node_group_3
storage_class = gp2

[ec2_node_group_1]
instance_type = c5.12xlarge
instance_capacity = 4
volume_size = 1000

[ec2_node_group_2]
instance_type = c5.12xlarge
instance_capacity = 1
volume_size = 1000

[ec2_node_group_3]
instance_type = c5.2xlarge
instance_capacity = 200
volume_size = 100

[storage]
data = /data

[credentials]
rest = Administrator:password
ssh = root:couchbase
aws_key_name = korry

[parameters]
OS = CentOS 7
CPU = c5.12xlarge (48 vCPU)
Memory = 96 GB
Disk = EBS 1TB