output "cluster_instance_ips" {
  value = {
    for k, v in azurerm_virtual_machine.perf-cluster-vm: k => {
      node_group = v.tags["node_group"]
      public_ip  = azurerm_public_ip.perf-public-cluster[k].ip_address
      private_ip = azurerm_network_interface.perf-cluster-ni[k].private_ip_address
    }
  }
}

output "client_instance_ips" {
  value = {
    for k, v in azurerm_virtual_machine.perf-client-vm: k => {
      node_group = v.tags["node_group"]
      public_ip  = azurerm_public_ip.perf-public-client[k].ip_address
      private_ip = azurerm_network_interface.perf-client-ni[k].private_ip_address
    }
  }
}

output "utility_instance_ips" {
  value = {
    for k, v in azurerm_virtual_machine.perf-utility-vm: k => {
      node_group = v.tags["node_group"]
      public_ip  = azurerm_public_ip.perf-public-utility[k].ip_address
      private_ip = azurerm_network_interface.perf-utility-ni[k].private_ip_address
    }
  }
}

output "syncgateway_instance_ips" {
  value = {
    for k, v in azurerm_virtual_machine.perf-syncgateway-vm: k => {
      node_group = v.tags["node_group"]
      public_ip  = azurerm_public_ip.perf-public-syncgateway[k].ip_address
      private_ip = azurerm_network_interface.perf-syncgateway-ni[k].private_ip_address
    }
  }
}

output "network" {
  value = {
    vpc_id =      azurerm_virtual_network.perf-vn.id
    subnet_cidr = one(azurerm_subnet.perf-sn.address_prefixes)
  }
}

output "cloud_storage" {
  value = {
    storage_account = length(azurerm_storage_account.perf-storage-acc) != 0 ? one(azurerm_storage_account.perf-storage-acc).name : null
    storage_bucket  = length(azurerm_storage_container.perf-storage-container) != 0 ? "az://${azurerm_storage_container.perf-storage-container[0].name}" : null
  }
  sensitive = true
}
