output "cluster_public_ip" {

  value = "${azurerm_public_ip.perf-public-cluster.*.ip_address}"
}
output "clients_public_ip" {

  value = "${azurerm_public_ip.perf-public-client.*.ip_address}"
}
output "utilities_public_ip" {

  value = "${azurerm_public_ip.perf-public-utility.*.ip_address}"
}
output "cluster_private_ip" {

  value = "${azurerm_network_interface.perf-cluster-ni.*.private_ip_address}"
}
output "clients_private_ip" {

  value = "${azurerm_network_interface.perf-client-ni.*.private_ip_address}"
}
output "utilities_private_ip" {

  value = "${azurerm_network_interface.perf-utility-ni.*.private_ip_address}"
}

