terraform {

   required_version = ">=0.12"

   required_providers {
     azurerm = {
       source = "hashicorp/azurerm"
       version = "~>2.0"
     }
   }
 }

 provider "azurerm" {
   features {}
 }

# Create virtual network.
 resource "azurerm_virtual_network" "perf-vn" {
   name                = "perf-vn-SUFFIX"
   address_space       = ["10.0.0.0/16"]
   location            = "East US"
   resource_group_name = "perf-resources-eastus"
 }

# Create subnet.
 resource "azurerm_subnet" "perf-sn" {
   name                 = "perf-sn-SUFFIX"
   resource_group_name  = "perf-resources-eastus"
   virtual_network_name = azurerm_virtual_network.perf-vn.name
   address_prefixes     = ["10.0.2.0/24"]
 }

# Create public cluster ip(s).
resource "azurerm_public_ip" "perf-public-cluster" {
  count               = CLUSTER_CAPACITY
  name                = "cluster-pip-${count.index}-SUFFIX"
  location            = "East US"
  resource_group_name = "perf-resources-eastus"
  allocation_method   = "Static"
}

# Create public client ip(s).
resource "azurerm_public_ip" "perf-public-client" {
  count               = CLIENTS_CAPACITY
  name                = "client-pip-${count.index}-SUFFIX"
  location            = "East US"
  resource_group_name = "perf-resources-eastus"
  allocation_method   = "Static"
}

# Create public utility ip(s).
resource "azurerm_public_ip" "perf-public-utility" {
  count               = UTILITIES_CAPACITY
  name                = "utility-pip-${count.index}-SUFFIX"
  location            = "East US"
  resource_group_name = "perf-resources-eastus"
  allocation_method   = "Static"
}

# Create network interface, map public ips.
resource "azurerm_network_interface" "perf-cluster-ni" {
  name                          = "perf-cluster-ni${count.index}-SUFFIX"
  location                      = "East US"
  resource_group_name           = "perf-resources-eastus"
  count                         = CLUSTER_CAPACITY
  enable_accelerated_networking = true

  ip_configuration {
    name                          = "perf-pip-cluster-${count.index}-SUFFIX"
    subnet_id                     = azurerm_subnet.perf-sn.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.perf-public-cluster[count.index].id
  }
}

# Create network interface, map public ips.
resource "azurerm_network_interface" "perf-client-ni" {
  name                          = "perf-client-ni${count.index}-SUFFIX"
  location                      = "East US"
  resource_group_name           = "perf-resources-eastus"
  count                         = CLIENTS_CAPACITY
  enable_accelerated_networking = true

  ip_configuration {
    name                          = "perf-pip-client-${count.index}-SUFFIX"
    subnet_id                     = azurerm_subnet.perf-sn.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.perf-public-client[count.index].id
  }
}

# Create network interface, map public ips.
resource "azurerm_network_interface" "perf-utility-ni" {
  name                          = "perf-utility-ni${count.index}-SUFFIX"
  location                      = "East US"
  resource_group_name           = "perf-resources-eastus"
  count                         = UTILITIES_CAPACITY
  enable_accelerated_networking = true

  ip_configuration {
    name                          = "perf-pip-utility-${count.index}-SUFFIX"
    subnet_id                     = azurerm_subnet.perf-sn.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.perf-public-utility[count.index].id
  }
}

# Config and create cluster disk(s).
 resource "azurerm_managed_disk" "perf-cluster-disk" {
   count                = CLUSTER_CAPACITY
   name                 = "perf-cluster-disk-${count.index}-SUFFIX"
   location             = "East US"
   resource_group_name  = "perf-resources-eastus"
   storage_account_type = STORAGE_TYPE
   create_option        = "Empty"
   disk_size_gb         = CLUSTER_DISK
 }

# Config and create client disk(s).
 resource "azurerm_managed_disk" "perf-client-disk" {
   count                = CLIENTS_CAPACITY
   name                 = "perf-client-disk-${count.index}-SUFFIX"
   location             = "East US"
   resource_group_name  = "perf-resources-eastus"
   storage_account_type = STORAGE_TYPE
   create_option        = "Empty"
   disk_size_gb         = CLIENTS_DISK
 }

# Config and create utility disk(s).
 resource "azurerm_managed_disk" "perf-utility-disk" {
   count                = UTILITIES_CAPACITY
   name                 = "perf-utility-disk-${count.index}-SUFFIX"
   location             = "East US"
   resource_group_name  = "perf-resources-eastus"
   storage_account_type = STORAGE_TYPE
   create_option        = "Empty"
   disk_size_gb         = UTILITIES_DISK
 }

# Create cluster VMs.
 resource "azurerm_virtual_machine" "perf-cluster-vm" {
   count                 = CLUSTER_CAPACITY
   name                  = "perf-cluster-vm-${count.index}-SUFFIX"
   location              = "East US"
   resource_group_name   = "perf-resources-eastus"
   network_interface_ids = [element(azurerm_network_interface.perf-cluster-ni.*.id, count.index)]
   vm_size               = CLUSTER_INSTANCE

   # Uncomment this line to delete the OS disk automatically when deleting the VM
   delete_os_disk_on_termination = true

   # Uncomment this line to delete the data disks automatically when deleting the VM
   delete_data_disks_on_termination = true

   identity {
    type = "UserAssigned"
    identity_ids = ["/subscriptions/a5c0936c-5cec-4c8c-85e1-97f5cab644d9/resourceGroups/perf-resources-eastus/providers/Microsoft.ManagedIdentity/userAssignedIdentities/perfrunner-mi"]
}

   storage_image_reference {
     id = "/subscriptions/a5c0936c-5cec-4c8c-85e1-97f5cab644d9/resourceGroups/perf-resources-eastus/providers/Microsoft.Compute/galleries/perf_vm_images/images/perf-server-image-def"
   }

   storage_os_disk {
     name              = "perf-cluster-os-disk-${count.index}-SUFFIX"
     caching           = "ReadWrite"
     create_option     = "FromImage"
     managed_disk_type = STORAGE_TYPE
   }

   storage_data_disk {
     name            = element(azurerm_managed_disk.perf-cluster-disk.*.name, count.index)
     managed_disk_id = element(azurerm_managed_disk.perf-cluster-disk.*.id, count.index)
     create_option   = "Attach"
     lun             = 1
     disk_size_gb    = element(azurerm_managed_disk.perf-cluster-disk.*.disk_size_gb, count.index)
   }

   tags = {
     type = "clustervm"
     environment = "staging"
   }
 }

# Create client VM(s).
 resource "azurerm_virtual_machine" "perf-client-vm" {
   count                 = CLIENTS_CAPACITY
   name                  = "perf-client-vm-${count.index}-SUFFIX"
   location              = "East US"
   resource_group_name   = "perf-resources-eastus"
   network_interface_ids = [element(azurerm_network_interface.perf-client-ni.*.id, count.index)]
   vm_size               = CLIENTS_INSTANCE

   # Uncomment this line to delete the OS disk automatically when deleting the VM
   delete_os_disk_on_termination = true

   # Uncomment this line to delete the data disks automatically when deleting the VM
   delete_data_disks_on_termination = true

   identity {
    type = "UserAssigned"
    identity_ids = ["/subscriptions/a5c0936c-5cec-4c8c-85e1-97f5cab644d9/resourceGroups/perf-resources-eastus/providers/Microsoft.ManagedIdentity/userAssignedIdentities/perfrunner-mi"]
}

   storage_image_reference {
     id = "/subscriptions/a5c0936c-5cec-4c8c-85e1-97f5cab644d9/resourceGroups/perf-resources-eastus/providers/Microsoft.Compute/galleries/perf_vm_images/images/perf-client-image-def/versions/1.0.0"
   }

   storage_os_disk {
     name              = "perf-client-os-disk-${count.index}-SUFFIX"
     caching           = "ReadWrite"
     create_option     = "FromImage"
     managed_disk_type = STORAGE_TYPE
   }

   storage_data_disk {
     name            = element(azurerm_managed_disk.perf-client-disk.*.name, count.index)
     managed_disk_id = element(azurerm_managed_disk.perf-client-disk.*.id, count.index)
     create_option   = "Attach"
     lun             = 1
     disk_size_gb    = element(azurerm_managed_disk.perf-client-disk.*.disk_size_gb, count.index)
   }

   tags = {
     type = "clustervm"
     environment = "staging"
   }
 }

# Create utility VM(s).
 resource "azurerm_virtual_machine" "perf-utility-vm" {
   count                 = UTILITIES_CAPACITY
   name                  = "perf-utility-vm-${count.index}-SUFFIX"
   location              = "East US"
   resource_group_name   = "perf-resources-eastus"
   network_interface_ids = [element(azurerm_network_interface.perf-utility-ni.*.id, count.index)]
   vm_size               = UTILITIES_INSTANCE

   # Uncomment this line to delete the OS disk automatically when deleting the VM
   delete_os_disk_on_termination = true

   # Uncomment this line to delete the data disks automatically when deleting the VM
   delete_data_disks_on_termination = true

   storage_image_reference {
     id = "/subscriptions/a5c0936c-5cec-4c8c-85e1-97f5cab644d9/resourceGroups/perf-resources-eastus/providers/Microsoft.Compute/galleries/perf_vm_images/images/perf-broker-image-def/versions/1.0.0"
   }

   storage_os_disk {
     name              = "perf-utility-os-disk-${count.index}-SUFFIX"
     caching           = "ReadWrite"
     create_option     = "FromImage"
     managed_disk_type = STORAGE_TYPE
   }

   storage_data_disk {
     name            = element(azurerm_managed_disk.perf-utility-disk.*.name, count.index)
     managed_disk_id = element(azurerm_managed_disk.perf-utility-disk.*.id, count.index)
     create_option   = "Attach"
     lun             = 1
     disk_size_gb    = element(azurerm_managed_disk.perf-utility-disk.*.disk_size_gb, count.index)
   }

   tags = {
     type = "clustervm"
     environment = "staging"
   }
 }

resource "azurerm_storage_account" "perf-storage-acc" {
  count                    = BACKUP
  name                     = "perfstorageaccSUFFIX"
  resource_group_name      = "perf-resources-eastus"
  location                 = "East US"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  access_tier              = "Cool"
}

resource "azurerm_storage_container" "perf-storage-container" {
  count                 = BACKUP
  name                  = "contentSUFFIX"
  storage_account_name  = element(azurerm_storage_account.perf-storage-acc.*.name, count.index)
  container_access_type = "private"
}


