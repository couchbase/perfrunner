output "cluster_instance_ips" {
    value = {
        for k, v in google_compute_instance.cluster_instance: k => {
            node_group = "${join("_", slice(split("-", split("/", v.id)[5]), 1, 5))}.${split("-", split("/", v.id)[5])[5]}"
            public_ip  = v.network_interface.0.access_config.0.nat_ip
            private_ip = v.network_interface.0.network_ip
        }
    }
}

output "client_instance_ips" {
    value = {
        for k, v in google_compute_instance.client_instance: k => {
            node_group = "${join("_", slice(split("-", split("/", v.id)[5]), 1, 5))}.${split("-", split("/", v.id)[5])[5]}"
            public_ip  = v.network_interface.0.access_config.0.nat_ip
            private_ip = v.network_interface.0.network_ip
        }
    }
}

output "utility_instance_ips" {
    value = {
        for k, v in google_compute_instance.utility_instance: k => {
            node_group = "${join("_", slice(split("-", split("/", v.id)[5]), 1, 5))}.${split("-", split("/", v.id)[5])[5]}"
            public_ip  = v.network_interface.0.access_config.0.nat_ip
            private_ip = v.network_interface.0.network_ip
        }
    }
}

output "sync_gateway_instance_ips" {
    value = {
        for k, v in google_compute_instance.sync_gateway_instance: k => {
            node_group = "${join("_", slice(split("-", split("/", v.id)[5]), 1, 5))}.${split("-", split("/", v.id)[5])[5]}"
            public_ip  = v.network_interface.0.access_config.0.nat_ip
            private_ip = v.network_interface.0.network_ip
        }
    }
}

output "network" {
    value = {
        vpc_id      = google_compute_network.perf-vn.name
        subnet_cidr = google_compute_subnetwork.perf-sn.ip_cidr_range
    }
}

output "cloud_storage" {
    value = {
        storage_bucket = length(google_storage_bucket.perf-storage-bucket) != 0 ? one(google_storage_bucket.perf-storage-bucket).url : null
    }
}
