output "cluster_public_ip" {
  value = "${google_compute_instance.perf-cluster-vm.*.network_interface.0.access_config.0.nat_ip}"
}

output "clients_public_ip" {
  value = "${google_compute_instance.perf-client-vm.*.network_interface.0.access_config.0.nat_ip}"
}

output "utilities_public_ip" {
  value = "${google_compute_instance.perf-utility-vm.*.network_interface.0.access_config.0.nat_ip}"
}

output "cluster_private_ip" {
  value = "${google_compute_instance.perf-cluster-vm.*.network_interface.0.network_ip}"
}

output "clients_private_ip" {
  value = "${google_compute_instance.perf-client-vm.*.network_interface.0.network_ip}"
}

output "utilities_private_ip" {
  value = "${google_compute_instance.perf-utility-vm.*.network_interface.0.network_ip}"
}