terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.21.0"
    }
  }
}

variable "cloud_region" {
  type = string
}

variable "cloud_zone" {
  type = string
}

variable "uuid" {
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
    local_nvmes       = number
  }))
}

variable "client_nodes" {
  type = map(object({
    node_group    = string
    image         = string
    instance_type = string
    storage_class = string
    volume_size   = number
    iops          = number
  }))
}

variable "utility_nodes" {
  type = map(object({
    image         = string
    instance_type = string
  }))
}

variable "syncgateway_nodes" {
  type = map(object({
    node_group    = string
    image         = string
    instance_type = string
    storage_class = string
    volume_size   = number
    iops          = number
  }))
}

variable "cloud_storage" {
  type = bool
}

variable "global_tag" {
  type = string
}

variable "allowed_ips" {
  type = list(string)
}

provider "google" {
  project = "couchbase-qe"
  region  = var.cloud_region
  zone    = var.cloud_zone
}

resource "google_compute_network" "perf-vn" {
  name                    = "perf-vn-${var.uuid}"
  auto_create_subnetworks = "false"
}

resource "google_compute_subnetwork" "perf-sn" {
  name          = "perf-sn-${var.uuid}"
  ip_cidr_range = "10.1.0.0/20"
  network       = google_compute_network.perf-vn.id
}

resource "google_compute_firewall" "allow-node-to-node" {
  name    = "allow-node-to-node-${var.uuid}"
  network = google_compute_network.perf-vn.name

  direction = "INGRESS"

  allow {
    protocol = "all"
  }

  source_ranges = concat(
    [google_compute_subnetwork.perf-sn.ip_cidr_range],
    [for vm in google_compute_instance.client_instance: vm.network_interface.0.access_config.0.nat_ip],
    [for vm in google_compute_instance.cluster_instance: vm.network_interface.0.access_config.0.nat_ip],
    [for vm in google_compute_instance.syncgateway_instance: vm.network_interface.0.access_config.0.nat_ip],
  )
}

resource "google_compute_firewall" "allow-ssh" {
  name    = "allow-ssh-${var.uuid}"
  network = google_compute_network.perf-vn.name

  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = var.allowed_ips
}

resource "google_compute_firewall" "allow-broker" {
  name    = "allow-broker-${var.uuid}"
  network = google_compute_network.perf-vn.name

  direction = "INGRESS"

  target_tags = ["utility"]

  allow {
    protocol = "tcp"
    ports    = ["5672"]
  }

  source_ranges = var.allowed_ips
}

resource "google_compute_firewall" "allow-couchbase" {
  name    = "allow-couchbase-${var.uuid}"
  network = google_compute_network.perf-vn.name

  direction = "INGRESS"

  target_tags = ["cluster"]

  allow {
    protocol = "tcp"
    ports    = ["4894-5025", "8091-8096", "9102", "9110", "18091-18096", "19102", "19110", "11207", "11209-11210"]
  }

  source_ranges = var.allowed_ips
}

resource "google_compute_instance" "cluster_instance" {
  for_each = var.cluster_nodes

  name         = "cluster-${replace(replace(each.value.node_group, ".", "-"), "_", "-")}-vm-${var.uuid}"
  machine_type = "${each.value.instance_type}"

  tags = ["cluster"]

  labels = {
    role       = "cluster"
    node_group = replace(each.value.node_group, ".", "-")
    deployment = var.global_tag != "" ? var.global_tag : null
  }

  boot_disk {
    initialize_params {
      size = "50"
      image = each.value.image
    }
  }

  dynamic "attached_disk"{
    for_each = (
      try(google_compute_disk.cluster-disk[each.key].id, null) != null
    ) ? [google_compute_disk.cluster-disk[each.key].id] : []

    content {
      source = attached_disk.value
    }
  }

  dynamic "scratch_disk" {
    for_each = range(each.value.local_nvmes)
    content {
      interface = "NVME"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.perf-sn.id
    access_config {
      network_tier = "PREMIUM"
    }
  }

  service_account {
    email  = "perftest-tools@couchbase-qe.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_disk" "cluster-disk" {
  for_each = {for k, node in var.cluster_nodes : k => node if node.volume_size > 0}

  name                   = "cluster-data-disk-${each.key}-${var.uuid}"
  type                   = lower(each.value.storage_class)
  size                   = each.value.volume_size
  provisioned_iops       = each.value.iops > 0 ? each.value.iops : null
  provisioned_throughput = each.value.volume_throughput > 0 ? each.value.volume_throughput : null
  labels = {
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

resource "google_compute_instance" "client_instance" {
  for_each = var.client_nodes

  name         = "client-${replace(replace(each.value.node_group, ".", "-"), "_", "-")}-vm-${var.uuid}"
  machine_type = "${each.value.instance_type}"

  tags = ["client"]

  labels = {
    role       = "client"
    node_group = replace(each.value.node_group, ".", "-")
    deployment = var.global_tag != "" ? var.global_tag : null
  }

  boot_disk {
    initialize_params {
      size = "50"
      type = "pd-balanced"
      image = each.value.image
    }
  }

  dynamic "attached_disk"{
    for_each = (
      try(google_compute_disk.client-disk[each.key].id, null) != null
    ) ? [google_compute_disk.client-disk[each.key].id] : []

    content {
      source = attached_disk.value
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.perf-sn.id
    access_config {
      network_tier = "PREMIUM"
    }
  }

  service_account {
    email  = "perftest-tools@couchbase-qe.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_disk" "client-disk" {
  for_each = {for k, node in var.client_nodes : k => node if node.volume_size > 0}

  name             = "client-data-disk-${each.key}-${var.uuid}"
  type             = lower(each.value.storage_class)
  size             = each.value.volume_size
  provisioned_iops = each.value.iops > 0 ? each.value.iops : null
  labels = {
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

resource "google_compute_instance" "utility_instance" {
  for_each = var.utility_nodes

  name         = "utility-vm-${var.uuid}"
  machine_type = "${each.value.instance_type}"

  tags = ["utility"]

  labels = {
    role       = "utility"
    deployment = var.global_tag != "" ? var.global_tag : null
  }

  boot_disk {
    initialize_params {
      size = "50"
      type = "pd-balanced"
      image = each.value.image
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.perf-sn.id
    access_config {
      network_tier = "PREMIUM"
    }
  }

  service_account {
    email  = "perftest-tools@couchbase-qe.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_instance" "syncgateway_instance" {
  for_each = var.syncgateway_nodes

  name         = "syncgateway-${replace(replace(each.value.node_group, ".", "-"), "_", "-")}-vm-${var.uuid}"
  machine_type = "${each.value.instance_type}"

  tags = ["syncgateway"]

  labels = {
    role       = "syncgateway"
    node_group = replace(each.value.node_group, ".", "-")
    deployment = var.global_tag != "" ? var.global_tag : null
  }

  boot_disk {
    initialize_params {
      size = "50"
      type = "pd-balanced"
      image = each.value.image
    }
  }

  dynamic "attached_disk"{
    for_each = (
      try(google_compute_disk.syncgateway-disk[each.key].id, null) != null
    ) ? [google_compute_disk.syncgateway-disk[each.key].id] : []

    content {
      source = attached_disk.value
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.perf-sn.id
    access_config {
      network_tier = "PREMIUM"
    }
  }

  service_account {
    email  = "perftest-tools@couchbase-qe.iam.gserviceaccount.com"
    scopes = ["cloud-platform"]
  }
}

resource "google_compute_disk" "syncgateway-disk" {
  for_each = {for k, node in var.syncgateway_nodes : k => node if node.volume_size > 0}

  name             = "syncgateway-data-disk-${each.key}-${var.uuid}"
  type             = lower(each.value.storage_class)
  size             = each.value.volume_size
  provisioned_iops = each.value.iops > 0 ? each.value.iops : null
  labels = {
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

resource "google_storage_bucket" "perf-storage-bucket" {
  count                       = var.cloud_storage ? 1 : 0
  name                        = "perftest-bucket-${var.uuid}"
  location                    = upper(var.cloud_region)
  uniform_bucket_level_access = true
  force_destroy               = true
  labels = {
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}
