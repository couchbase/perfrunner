output "cluster_instance_ips" {
  value = {
    for k, v in aws_instance.cluster_instance: k => {
      node_group = v.tags_all["node_group"]
      public_ip  = v.public_dns
    }
  }
}

output "client_instance_ips" {
  value = {
    for k, v in aws_instance.client_instance: k => {
      node_group = v.tags_all["node_group"]
      public_ip  = v.public_dns
    }
  }
}

output "utility_instance_ips" {
  value = {
    for k, v in aws_instance.utility_instance: k => {
      node_group = v.tags_all["node_group"]
      public_ip  = v.public_dns
    }
  }
}

output "syncgateway_instance_ips" {
  value = {
    for k, v in aws_instance.syncgateway_instance: k => {
      node_group = v.tags_all["node_group"]
      public_ip  = v.public_dns
    }
  }
}

output "kafka_instance_ips" {
  value = {
    for k, v in aws_instance.kafka_instance: k => {
      node_group = v.tags_all["node_group"]
      public_ip  = v.public_dns
      subnet_id  = aws_subnet.kafka_private[k].id
    }
  }
}

output "network" {
  value = {
    vpc_id                   = aws_vpc.main.id
    public_subnet_id         = aws_subnet.public.id
    public_subnet_cidr       = aws_subnet.public.cidr_block
  }
}

output "cloud_storage" {
  value = {
    storage_bucket = length(aws_s3_bucket.perf-storage-bucket) != 0 ? "s3://${one(aws_s3_bucket.perf-storage-bucket).id}" : null
  }
}
