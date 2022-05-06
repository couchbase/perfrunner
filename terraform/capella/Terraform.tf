terraform {
  required_providers {
    couchbasecapella = {
      source  = "terraform.couchbase.com/local/couchbasecapella"
      version = "1.0.0"
    }
  }
}
provider "couchbasecapella" {
}

# Creates hosted capella cluster.
resource "couchbasecapella_hosted_cluster" "cluster" {
  name        = "perf-cluster"
  project_id  = "8de5c1bf-9647-49d6-a124-c9998ccdc1cd"
  description = "Perf-Hosted-Cluster"
  place {
    single_az = true
    hosted {
      provider = "aws"
      region   = "us-east-1"
      cidr     = "10.0.16.0/20"
    }
  }
  support_package {
    timezone             = "GMT"
    support_package_type = "Basic"
  }
  servers {
    size     = CLUSTER_CAPACITY
    compute  = CLUSTER_INSTANCE
    services = ["data"]
    storage {
      storage_type = STORAGE_TYPE
      iops         = "3000"
      storage_size = CLUSTER_DISK
    }
  }
}
