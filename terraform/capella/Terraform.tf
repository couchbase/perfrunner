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

variable "uuid" {
    type = string
}

variable "cluster_settings" {
    type = object({
        project_id = string
        provider   = string
        region     = string
        cidr       = string
    })
}

variable "server_groups" {
    type = list(object({
        instance_capacity = number
        instance_type     = string
        services          = list(string)
        storage_class     = string
        volume_size       = number
        iops              = number
    }))
}

# Creates hosted capella cluster.
resource "couchbasecapella_hosted_cluster" "cluster" {
    name        = "perf-cluster-${var.uuid}"
    project_id  = var.cluster_settings.project_id
    description = "performance testing"
    place {
        single_az = true
        hosted {
            provider = var.cluster_settings.provider
            region   = var.cluster_settings.region
            cidr     = var.cluster_settings.cidr
        }
    }
    support_package {
        timezone             = "GMT"
        support_package_type = "DeveloperPro"
    }

    dynamic "servers" {
        for_each = var.server_groups
        content {
            size     = servers.value["instance_capacity"]
            compute  = servers.value["instance_type"]
            services = servers.value["services"]
            storage {
                storage_type = upper(servers.value["storage_class"])
                storage_size = servers.value["volume_size"]
                iops         = servers.value["iops"] > 0 ? servers.value["iops"] : null
            }
        }
    }
}
