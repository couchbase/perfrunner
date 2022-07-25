variable "cloud_region" {
  type = string
}

variable "cluster_nodes" {
  type = map(object({
    node_group      = string
    image           = string
    instance_type   = string
    storage_class   = string
    volume_size     = number
    iops            = number
    disk_throughput = number
  }))
}

variable "client_nodes" {
  type = map(object({
    node_group      = string
    image           = string
    instance_type   = string
    storage_class   = string
    volume_size     = number
    iops            = number
    disk_throughput = number
  }))
}

variable "utility_nodes" {
  type = map(object({
    node_group      = string
    image           = string
    instance_type   = string
    storage_class   = string
    volume_size     = number
    iops            = number
    disk_throughput = number
  }))
}

variable "sync_gateway_nodes" {
  type = map(object({
    node_group      = string
    image           = string
    instance_type   = string
    storage_class   = string
    volume_size     = number
    iops            = number
    disk_throughput = number
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
        [for k, v in var.sync_gateway_nodes : v.instance_type],
      )
    )
  }

  location_type = "availability-zone"
}

resource "random_shuffle" "az" {
  input        = distinct(data.aws_ec2_instance_type_offerings.available.locations)
  result_count = 1
}

resource "aws_vpc" "main"{
  cidr_block           = "10.1.0.0/18"
  enable_dns_hostnames = true
  tags = {
    Name = "TerraVPC"
  }
}

resource "aws_security_group_rule" "enable_ssh" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_vpc.main.default_security_group_id
}

resource "aws_security_group_rule" "enable_rabbitmq" {
  type              = "ingress"
  from_port         = 5672
  to_port           = 5672
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_vpc.main.default_security_group_id
}

# ["8091-8096","18091-18096","11210","11207"]
resource "aws_security_group_rule" "enable_couchbase_default" {
  type              = "ingress"
  from_port         = 8091
  to_port           = 8096
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_vpc.main.default_security_group_id
}

resource "aws_security_group_rule" "enable_couchbase_secure" {
  type              = "ingress"
  from_port         = 18091
  to_port           = 18096
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_vpc.main.default_security_group_id
}

resource "aws_security_group_rule" "enable_memcached" {
  type              = "ingress"
  from_port         = 11210
  to_port           = 11210
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_vpc.main.default_security_group_id
}

resource "aws_security_group_rule" "enable_memcached_secure" {
  type              = "ingress"
  from_port         = 11207
  to_port           = 11207
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_vpc.main.default_security_group_id
}

#Public subnet
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  availability_zone       = one(random_shuffle.az.result)
  cidr_block              = "10.1.0.0/24"
  map_public_ip_on_launch = true
  tags = {
    Name = "Public Subnet"
  }
}

#Internet Gateway
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
  tags = {
    Name = "Terra IGW"
  }
}

#Elastic IP for NAT Gateway
resource "aws_eip" "nat_eip" {
  vpc        = true
  depends_on = [aws_internet_gateway.igw]
  tags = {
    Name = "Terra NAT Gateway EIP"
  }
}

#Main NAT Gateway for VPC
resource "aws_nat_gateway" "nat"{
  allocation_id = aws_eip.nat_eip.id
  subnet_id     = aws_subnet.public.id
  tags = {
    Name = "NAT GW"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
}

#Map public subnet to public route table.
resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_instance" "cluster_instance" {
  for_each = var.cluster_nodes

  availability_zone = one(random_shuffle.az.result)
  subnet_id         = aws_subnet.public.id
  ami               = each.value.image
  instance_type     = each.value.instance_type
  ebs_block_device {
    device_name = "/dev/sdb"
    volume_size = each.value.volume_size
    volume_type = lower(each.value.storage_class)
  }
  tags = {
    role       = "cluster"
    node_group = each.value.node_group
    perfrunner = var.global_tag != "" ? var.global_tag : null
  }
}

resource "aws_instance" "client_instance" {
  for_each = var.client_nodes

  availability_zone = one(random_shuffle.az.result)
  subnet_id         = aws_subnet.public.id
  ami               = each.value.image
  instance_type     = each.value.instance_type
  ebs_block_device {
    device_name = "/dev/sdb"
    volume_size = each.value.volume_size
    volume_type = lower(each.value.storage_class)
  }
  tags = {
    role       = "client"
    node_group = each.value.node_group
    perfrunner = var.global_tag != "" ? var.global_tag : null
  }
}

resource "aws_instance" "utility_instance" {
  for_each = var.utility_nodes

  availability_zone = one(random_shuffle.az.result)
  subnet_id         = aws_subnet.public.id
  ami               = each.value.image
  instance_type     = each.value.instance_type
  ebs_block_device {
    device_name = "/dev/sdb"
    volume_size = each.value.volume_size
    volume_type = lower(each.value.storage_class)
  }
  tags = {
    role       = "utility"
    node_group = each.value.node_group
    perfrunner = var.global_tag != "" ? var.global_tag : null
  }
}

resource "aws_instance" "sync_gateway_instance" {
  for_each = var.sync_gateway_nodes

  availability_zone = one(random_shuffle.az.result)
  subnet_id         = aws_subnet.public.id
  ami               = each.value.image
  instance_type     = each.value.instance_type
  ebs_block_device {
    device_name = "/dev/sdb"
    volume_size = each.value.volume_size
    volume_type = lower(each.value.storage_class)
  }
  tags = {
    role       = "sync_gateway"
    node_group = each.value.node_group
    perfrunner = var.global_tag != "" ? var.global_tag : null
  }
}

resource "aws_s3_bucket" "perf-storage-bucket" {
  count         = var.cloud_storage ? 1 : 0
  bucket        = "perftest-bucket-${substr(uuid(), 0, 6)}"
  force_destroy = true
}
