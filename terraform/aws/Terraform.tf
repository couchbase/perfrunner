variable "cloud_region" {
  type = string
}

variable "cloud_zone" {
  type = string
}

variable "cluster_nodes" {
  type = map(object({
    node_group        = string
    image             = string
    instance_type     = string
    storage_class     = string
    volume_size       = number
    iops              = number
    volume_throughput = number
  }))
}

variable "client_nodes" {
  type = map(object({
    node_group        = string
    image             = string
    instance_type     = string
    storage_class     = string
    volume_size       = number
    iops              = number
    volume_throughput = number
  }))
}

variable "utility_nodes" {
  type = map(object({
    node_group        = string
    image             = string
    instance_type     = string
    storage_class     = string
    volume_size       = number
    iops              = number
    volume_throughput = number
  }))
}

variable "syncgateway_nodes" {
  type = map(object({
    node_group        = string
    image             = string
    instance_type     = string
    storage_class     = string
    volume_size       = number
    iops              = number
    volume_throughput = number
  }))
}

variable "kafka_nodes" {
  type = map(object({
    node_group        = string
    image             = string
    instance_type     = string
    storage_class     = string
    volume_size       = number
    iops              = number
    volume_throughput = number
  }))
}

variable "cloud_storage" {
  type = bool
}

variable "global_tag" {
  type = string
}

provider "aws" {
  region = var.cloud_region
}

data "aws_ec2_instance_type_offerings" "available" {
  filter {
    name = "instance-type"
    values = distinct(
      concat(
        [for k, v in var.cluster_nodes : v.instance_type],
        [for k, v in var.client_nodes : v.instance_type],
        [for k, v in var.utility_nodes : v.instance_type],
        [for k, v in var.syncgateway_nodes : v.instance_type]
      )
    )
  }

  location_type = "availability-zone"
}

data "aws_ec2_instance_type_offerings" "available_kafka" {
  filter {
    name = "instance-type"
    values = distinct([for k, v in var.kafka_nodes : v.instance_type])
  }

  location_type = "availability-zone"
}

data "aws_ami" "cluster_ami" {
  for_each = var.cluster_nodes

  owners = ["self"]

  filter {
    name = "name"
    values = [each.value.image]
  }
}

data "aws_ami" "client_ami" {
  for_each = var.client_nodes

  owners = ["self"]

  filter {
    name = "name"
    values = [each.value.image]
  }
}

data "aws_ami" "utility_ami" {
  for_each = var.utility_nodes

  owners = ["self"]

  filter {
    name = "name"
    values = [each.value.image]
  }
}

data "aws_ami" "syncgateway_ami" {
  for_each = var.syncgateway_nodes

  owners = ["self"]

  filter {
    name = "name"
    values = [each.value.image]
  }
}

data "aws_ami" "kafka_ami" {
  for_each = var.kafka_nodes

  owners = ["self"]

  filter {
    name = "name"
    values = [each.value.image]
  }
}

resource "random_shuffle" "az" {
  count = (
    (
      length(data.aws_ec2_instance_type_offerings.available.instance_types) != 0 &&
      length(data.aws_ec2_instance_type_offerings.available.locations) != 0
    ) || var.cloud_zone != ""
  ) ? 1 : 0

  input = var.cloud_zone != "" ? [var.cloud_zone] : setintersection([
    for instance_type in distinct(data.aws_ec2_instance_type_offerings.available.instance_types) : [
      for idx, loc in data.aws_ec2_instance_type_offerings.available.locations : loc
        if data.aws_ec2_instance_type_offerings.available.instance_types[idx] == instance_type
    ]
  ]...)
  result_count = 1
}

resource "random_shuffle" "kafka_broker_azs" {
  count = length(var.kafka_nodes) != 0 ? 1 : 0
  input = setintersection([
    for instance_type in distinct(data.aws_ec2_instance_type_offerings.available_kafka.instance_types) : [
      for idx, loc in data.aws_ec2_instance_type_offerings.available_kafka.locations : loc
        if data.aws_ec2_instance_type_offerings.available_kafka.instance_types[idx] == instance_type
    ]
  ]...)
  result_count = length(var.kafka_nodes)
}

resource "aws_vpc" "main"{
  count = (length(data.aws_ec2_instance_type_offerings.available.instance_types) != 0 &&
           length(data.aws_ec2_instance_type_offerings.available.locations) != 0) ? 1 : 0

  cidr_block           = "10.1.0.0/18"
  enable_dns_hostnames = true
  tags = {
    Name = var.global_tag != "" ? var.global_tag : "TerraVPC"
  }
}

# Public subnet for Couchbase cluster
resource "aws_subnet" "public" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  vpc_id                  = one(aws_vpc.main[*].id)
  availability_zone       = one(random_shuffle.az[*].result)[0]
  cidr_block              = "10.1.0.0/24"
  map_public_ip_on_launch = true
  tags = {
    Name = var.global_tag != "" ? var.global_tag : "Public Subnet"
  }
}

# Private subnet(s) for Kafka brokers
resource "aws_subnet" "kafka_private" {
  for_each = var.kafka_nodes

  vpc_id                  = one(aws_vpc.main[*].id)
  availability_zone       = random_shuffle.kafka_broker_azs[0].result[tonumber(each.key) - 1]
  cidr_block              = "10.1.${each.key}.0/24"
  map_public_ip_on_launch = true
  tags = {
    Name = var.global_tag != "" ? "${var.global_tag}-private-${each.key}" : "Kafka Private Subnet"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "igw" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  vpc_id = one(aws_vpc.main[*].id)
  tags = {
    Name = var.global_tag != "" ? var.global_tag : "Terra IGW"
  }
}

# NAT Gateway for public subnet
resource "aws_nat_gateway" "nat"{
  count = length(aws_subnet.public) != 0 ? 1 : 0

  allocation_id = one(aws_eip.nat_eip[*].id)
  subnet_id     = one(aws_subnet.public[*].id)
  tags = {
    Name = var.global_tag != "" ? var.global_tag : "NAT GW"
  }
}

# Elastic IP for NAT Gateway
resource "aws_eip" "nat_eip" {
  count = length(aws_internet_gateway.igw) != 0 ? 1 : 0

  domain     = "vpc"
  depends_on = [aws_internet_gateway.igw]
  tags = {
    Name = var.global_tag != "" ? var.global_tag : "Terra NAT Gateway EIP"
  }
}

# Route table for public subnet
resource "aws_route_table" "public" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  vpc_id = one(aws_vpc.main[*].id)
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = one(aws_internet_gateway.igw[*].id)
  }
  tags = {
    Name = var.global_tag != "" ? var.global_tag : "Public Route Table"
  }
}

# Route table for private subnet(s)
resource "aws_route_table" "kafka_private" {
  count = length(var.kafka_nodes) != 0 ? 1 : 0
  vpc_id = one(aws_vpc.main[*].id)
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = one(aws_nat_gateway.nat[*].id)
  }
  tags = {
    Name = var.global_tag != "" ? "${var.global_tag}-private" : "Kafka Private Route Table"
  }
}

# Associate public subnet to public route table.
resource "aws_route_table_association" "public" {
  count = length(aws_subnet.public) != 0 ? 1 : 0

  subnet_id      = one(aws_subnet.public[*].id)
  route_table_id = one(aws_route_table.public[*].id)
}

# Associate private subnet(s) to private route table.
resource "aws_route_table_association" "kafka_private" {
  for_each = var.kafka_nodes
  subnet_id      = aws_subnet.kafka_private[each.key].id
  route_table_id = one(aws_route_table.kafka_private[*].id)
}

resource "aws_security_group_rule" "enable_ssh" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = one(aws_vpc.main[*].default_security_group_id)
}

resource "aws_security_group_rule" "enable_rabbitmq" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  type              = "ingress"
  from_port         = 5672
  to_port           = 5672
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = one(aws_vpc.main[*].default_security_group_id)
}

# ["8091-8096","18091-18096","11210","11207"]
resource "aws_security_group_rule" "enable_couchbase_default" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  type              = "ingress"
  from_port         = 8091
  to_port           = 8096
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = one(aws_vpc.main[*].default_security_group_id)
}

resource "aws_security_group_rule" "enable_indexer" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  type              = "ingress"
  from_port         = 9102
  to_port           = 9102
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = one(aws_vpc.main[*].default_security_group_id)
}

resource "aws_security_group_rule" "enable_cbas" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  type              = "ingress"
  from_port         = 9110
  to_port           = 9110
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = one(aws_vpc.main[*].default_security_group_id)
}

resource "aws_security_group_rule" "enable_couchbase_secure" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  type              = "ingress"
  from_port         = 18091
  to_port           = 18096
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = one(aws_vpc.main[*].default_security_group_id)
}

resource "aws_security_group_rule" "enable_indexer_secure" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  type              = "ingress"
  from_port         = 19102
  to_port           = 19102
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = one(aws_vpc.main[*].default_security_group_id)
}

resource "aws_security_group_rule" "enable_cbas_secure" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  type              = "ingress"
  from_port         = 19110
  to_port           = 19110
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = one(aws_vpc.main[*].default_security_group_id)
}

resource "aws_security_group_rule" "enable_memcached" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  type              = "ingress"
  from_port         = 11209
  to_port           = 11210
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = one(aws_vpc.main[*].default_security_group_id)
}

resource "aws_security_group_rule" "enable_memcached_secure" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  type              = "ingress"
  from_port         = 11207
  to_port           = 11207
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = one(aws_vpc.main[*].default_security_group_id)
}

resource "aws_security_group_rule" "enable_sgw" {
  count = length(aws_vpc.main) != 0 ? 1 : 0

  type              = "ingress"
  from_port         = 4984
  to_port           = 5025
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = one(aws_vpc.main[*].default_security_group_id)
}

resource "aws_instance" "cluster_instance" {
  for_each = var.cluster_nodes

  availability_zone = one(random_shuffle.az[*].result)[0]
  subnet_id         = one(aws_subnet.public[*].id)
  ami               = data.aws_ami.cluster_ami[each.key].id
  instance_type     = each.value.instance_type

  root_block_device {
    volume_size = 32
  }

  tags = {
    Name       = var.global_tag != "" ? var.global_tag : "ClusterNode${each.key}"
    role       = "cluster"
    node_group = each.value.node_group
  }
}

resource "aws_ebs_volume" "cluster_ebs_volume" {
  for_each = {for k, node in var.cluster_nodes : k => node if node.volume_size > 0}

  availability_zone = one(random_shuffle.az[*].result)[0]
  type              = lower(each.value.storage_class)
  size              = each.value.volume_size
  throughput        = each.value.volume_throughput > 0 ? each.value.volume_throughput : null
  iops              = each.value.iops > 0 ? each.value.iops : null

  tags = {
    Name = var.global_tag != "" ? "${var.global_tag}-cluster-ebs-${each.key}" : "ClusterEBS${each.key}"
  }
}

resource "aws_volume_attachment" "cluster_ebs_volume_attachment" {
  for_each = {for k, node in var.cluster_nodes : k => node if node.volume_size > 0}

  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.cluster_ebs_volume[each.key].id
  instance_id = aws_instance.cluster_instance[each.key].id
}

resource "aws_instance" "client_instance" {
  for_each = var.client_nodes

  availability_zone = one(random_shuffle.az[*].result)[0]
  subnet_id         = one(aws_subnet.public[*].id)
  ami               = data.aws_ami.client_ami[each.key].id
  instance_type     = each.value.instance_type

  tags = {
    Name       = var.global_tag != "" ? var.global_tag : "ClientNode${each.key}"
    role       = "client"
    node_group = each.value.node_group
  }
}

resource "aws_ebs_volume" "client_ebs_volume" {
  for_each = {for k, node in var.client_nodes : k => node if node.volume_size > 0}

  availability_zone = one(random_shuffle.az[*].result)[0]
  type              = lower(each.value.storage_class)
  size              = each.value.volume_size
  throughput        = each.value.volume_throughput > 0 ? each.value.volume_throughput : null
  iops              = each.value.iops > 0 ? each.value.iops : null

  tags = {
    Name = var.global_tag != "" ? "${var.global_tag}-client-ebs-${each.key}" : "ClientEBS${each.key}"
  }
}

resource "aws_volume_attachment" "client_ebs_volume_attachment" {
  for_each = {for k, node in var.client_nodes : k => node if node.volume_size > 0}

  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.client_ebs_volume[each.key].id
  instance_id = aws_instance.client_instance[each.key].id
}

resource "aws_instance" "utility_instance" {
  for_each = var.utility_nodes

  availability_zone = one(random_shuffle.az[*].result)[0]
  subnet_id         = one(aws_subnet.public[*].id)
  ami               = data.aws_ami.utility_ami[each.key].id
  instance_type     = each.value.instance_type

  tags = {
    Name       = var.global_tag != "" ? var.global_tag : "UtilityNode${each.key}"
    role       = "utility"
    node_group = each.value.node_group
  }
}

resource "aws_ebs_volume" "utility_ebs_volume" {
  for_each = {for k, node in var.utility_nodes : k => node if node.volume_size > 0}

  availability_zone = one(random_shuffle.az[*].result)[0]
  type              = lower(each.value.storage_class)
  size              = each.value.volume_size
  throughput        = each.value.volume_throughput > 0 ? each.value.volume_throughput : null
  iops              = each.value.iops > 0 ? each.value.iops : null

  tags = {
    Name = var.global_tag != "" ? "${var.global_tag}-utility-ebs-${each.key}" : "UtilityEBS${each.key}"
  }
}

resource "aws_volume_attachment" "utility_ebs_volume_attachment" {
  for_each = {for k, node in var.utility_nodes : k => node if node.volume_size > 0}

  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.utility_ebs_volume[each.key].id
  instance_id = aws_instance.utility_instance[each.key].id
}

resource "aws_instance" "syncgateway_instance" {
  for_each = var.syncgateway_nodes

  availability_zone = one(random_shuffle.az[*].result)[0]
  subnet_id         = one(aws_subnet.public[*].id)
  ami               = data.aws_ami.syncgateway_ami[each.key].id
  instance_type     = each.value.instance_type

  tags = {
    Name       = var.global_tag != "" ? var.global_tag : "SyncGatewayNode${each.key}"
    role       = "syncgateway"
    node_group = each.value.node_group
  }
}

resource "aws_ebs_volume" "syncgateway_ebs_volume" {
  for_each = {for k, node in var.syncgateway_nodes : k => node if node.volume_size > 0}

  availability_zone = one(random_shuffle.az[*].result)[0]
  type              = lower(each.value.storage_class)
  size              = each.value.volume_size
  throughput        = each.value.volume_throughput > 0 ? each.value.volume_throughput : null
  iops              = each.value.iops > 0 ? each.value.iops : null

  tags = {
    Name = var.global_tag != "" ? "${var.global_tag}-syncgateway-ebs-${each.key}" : "SyncGatewayEBS${each.key}"
  }
}

resource "aws_volume_attachment" "syncgateway_ebs_volume_attachment" {
  for_each = {for k, node in var.syncgateway_nodes : k => node if node.volume_size > 0}

  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.syncgateway_ebs_volume[each.key].id
  instance_id = aws_instance.syncgateway_instance[each.key].id
}

resource "aws_instance" "kafka_instance" {
  for_each = var.kafka_nodes

  availability_zone = random_shuffle.kafka_broker_azs[0].result[tonumber(each.key) - 1]
  subnet_id         = aws_subnet.kafka_private[each.key].id
  ami               = data.aws_ami.kafka_ami[each.key].id
  instance_type     = each.value.instance_type

  tags = {
    Name       = var.global_tag != "" ? var.global_tag : "KafkaNode${each.key}"
    role       = "kafka"
    node_group = each.value.node_group
  }
}

resource "aws_ebs_volume" "kafka_ebs_volume" {
  for_each = {for k, node in var.kafka_nodes : k => node if node.volume_size > 0}

  availability_zone = random_shuffle.kafka_broker_azs[0].result[tonumber(each.key) - 1]
  type              = lower(each.value.storage_class)
  size              = each.value.volume_size
  throughput        = each.value.volume_throughput > 0 ? each.value.volume_throughput : null
  iops              = each.value.iops > 0 ? each.value.iops : null

  tags = {
    Name = var.global_tag != "" ? "${var.global_tag}-kafka-ebs-${each.key}" : "KafkaEBS${each.key}"
  }
}

resource "aws_volume_attachment" "kafka_ebs_volume_attachment" {
  for_each = {for k, node in var.kafka_nodes : k => node if node.volume_size > 0}

  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.kafka_ebs_volume[each.key].id
  instance_id = aws_instance.kafka_instance[each.key].id
}

resource "aws_s3_bucket" "perf-storage-bucket" {
  count         = var.cloud_storage ? 1 : 0
  bucket        = "perftest-bucket-${substr(uuid(), 0, 6)}"
  force_destroy = true
  tags = {
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}
