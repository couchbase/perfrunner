provider "aws" {
  region = "us-east-1"
}

#VPC
resource "aws_vpc" "main"{
  cidr_block = "10.0.0.0/18"
  enable_dns_hostnames = true
  tags = {
    Name = "TerraVPC"
  }
}

#Public subnet
resource "aws_subnet" "public" {
  vpc_id = aws_vpc.main.id
  cidr_block = "10.0.0.0/24"
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
  vpc = true
  depends_on = [aws_internet_gateway.igw]
  tags = {
    Name = "Terra NAT Gateway EIP"
  }
}

#Main NAT Gateway for VPC
resource "aws_nat_gateway" "nat"{
  allocation_id = aws_eip.nat_eip.id
  subnet_id = aws_subnet.public.id
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
  subnet_id = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

#Create cluster VMs.
resource "aws_instance" "cluster" {
            subnet_id = aws_subnet.public.id
            ami = "ami-060e286353d227c32"
            instance_type = CLUSTER_INSTANCE
            count = CLUSTER_CAPACITY
}

#Create client VM(s).
resource "aws_instance" "clients" {
            subnet_id = aws_subnet.public.id
            ami = "ami-01b36cb3330d38ac5"
            instance_type = CLIENTS_INSTANCE
            count = CLIENTS_CAPACITY
}

#Create utility VM(s).
resource "aws_instance" "utilities" {
            subnet_id = aws_subnet.public.id
            ami = "ami-0d9e5ee360aa02d94"
            instance_type = UTILITIES_INSTANCE
            count = UTILITIES_CAPACITY
}