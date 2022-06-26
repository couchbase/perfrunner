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

output "sync_gateway_instance_ips" {
    value = {
        for k, v in aws_instance.sync_gateway_instance: k => {
            node_group = v.tags_all["node_group"]
            public_ip  = v.public_dns
        }
    }
}

output "cloud_storage" {
    value = {
        storage_bucket = length(aws_s3_bucket.perf-storage-bucket) != 0 ? "s3://${one(aws_s3_bucket.perf-storage-bucket).id}" : null
    }
}
