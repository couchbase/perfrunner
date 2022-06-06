provider "google" {
  project = "couchbase-qe"
  region  = "us-east1"
  zone    = "us-east1-c"
}

resource "google_compute_network" "perf-vn" {
  name                    = "perf-vn-SUFFIX"
  auto_create_subnetworks = "false"
}

resource "google_compute_subnetwork" "perf-sn" {
  name          = "perf-sn-SUFFIX"
  ip_cidr_range = "10.0.0.0/16"
  region        = "us-east1"
  network       = google_compute_network.perf-vn.id
}

resource "google_compute_firewall" "allow-custom" {
  name    = "allow-custom"
  network = google_compute_network.perf-vn.name

  direction = "INGRESS"

  allow {
    protocol = "all"
  }

  source_ranges = [google_compute_subnetwork.perf-sn.ip_cidr_range]
}

resource "google_compute_firewall" "allow-ssh" {
  name    = "allow-ssh"
  network = google_compute_network.perf-vn.name

  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_firewall" "allow-broker" {
  name    = "allow-broker"
  network = google_compute_network.perf-vn.name

  direction = "INGRESS"

  allow {
    protocol = "tcp"
    ports    = ["5672"]
  }

  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_firewall" "allow-couchbase" {
  name    = "allow-couchbase"
  network = google_compute_network.perf-vn.name

  direction = "INGRESS"

  target_tags = ["server"]

  allow {
    protocol = "tcp"
    ports    = ["8091-8096","18091-18096","11210","11207"]
  }

  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_disk" "perf-cluster-disk" {
  count = CLUSTER_CAPACITY
  name  = "perf-cluster-disk${count.index}-SUFFIX"
  type  = STORAGE_TYPE
  size  = CLUSTER_DISK
  zone  = "us-east1-c"
}

resource "google_compute_disk" "perf-client-disk" {
  count = CLIENTS_CAPACITY
  name  = "perf-client-disk${count.index}-SUFFIX"
  type  = STORAGE_TYPE
  size  = CLIENTS_DISK
  zone  = "us-east1-c"
}

resource "google_compute_instance" "perf-cluster-vm" {
  count        = CLUSTER_CAPACITY
  name         = "perf-cluster-vm${count.index}-SUFFIX"
  machine_type = CLUSTER_INSTANCE

  tags = ["server"]

  boot_disk {
    initialize_params {
      size = "50"
      type = "pd-balanced"
      image = "perftest-server-disk-image"
    }
  }

  attached_disk {
      source = element(google_compute_disk.perf-cluster-disk.*.id, count.index)
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

resource "google_compute_instance" "perf-client-vm" {
  count        = CLIENTS_CAPACITY
  name         = "perf-client-vm${count.index}-SUFFIX"
  machine_type = CLIENTS_INSTANCE
  tags = ["client"]

  boot_disk {
    initialize_params {
      size = "50"
      type = "pd-balanced"
      image = "perftest-client-disk-image-1"
    }
  }

   attached_disk {
      source = element(google_compute_disk.perf-client-disk.*.id, count.index)
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

resource "google_compute_instance" "perf-utility-vm" {
  count        = UTILITIES_CAPACITY
  name         = "perf-utility-vm${count.index}-SUFFIX"
  machine_type = UTILITIES_INSTANCE

  tags = ["utility"]

  boot_disk {
    initialize_params {
      size = "50"
      type = "pd-balanced"
      image = "perftest-broker-disk-image"
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

resource "google_storage_bucket" "perf-storage-bucket" {
  count         = BACKUP
  name          = "perf-storage-bucket-SUFFIX"
  location      = "US"
  force_destroy = true
  uniform_bucket_level_access = true
}