output "cluster_public_ip" {
  description = "Public IP address of the EC2 instance"
  value       = aws_instance.cluster.*.public_dns
}
output "clients_public_ip" {
  description = "Public IP address of the EC2 instance"
  value       = aws_instance.clients.*.public_dns
}
output "utilities_public_ip" {
  description = "Public IP address of the EC2 instance"
  value       = aws_instance.utilities.*.public_dns
}