provider "azurerm" {
  features {}
}

variable "uuid" {
  type = string
}

variable "cluster_nodes" {
  type = map(object({
    node_group    = string
    image         = string
    instance_type = string
    storage_class = string
    volume_size   = number
    disk_tier     = string
  }))
}

variable "client_nodes" {
  type = map(object({
    node_group    = string
    image         = string
    instance_type = string
    storage_class = string
    volume_size   = number
    disk_tier     = string
  }))
}

variable "utility_nodes" {
  type = map(object({
    node_group    = string
    image         = string
    instance_type = string
    storage_class = string
    volume_size   = number
    disk_tier     = string
  }))
}

variable "syncgateway_nodes" {
  type = map(object({
    node_group    = string
    image         = string
    instance_type = string
    storage_class = string
    volume_size   = number
    disk_tier     = string
  }))
}

variable "cloud_storage" {
  type = bool
}

variable "global_tag" {
  type = string
}

# Create virtual network.
resource "azurerm_virtual_network" "perf-vn" {
  name                = "perf-vn-${var.uuid}"
  address_space       = ["10.0.0.0/16"]
  location            = "East US"
  resource_group_name = "perf-resources-eastus"

  tags = {
  deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Create subnet.
resource "azurerm_subnet" "perf-sn" {
  name                 = "perf-sn-${var.uuid}"
  resource_group_name  = "perf-resources-eastus"
  virtual_network_name = azurerm_virtual_network.perf-vn.name
  address_prefixes     = ["10.0.2.0/24"]
}

# Create public cluster ip(s).
resource "azurerm_public_ip" "perf-public-cluster" {
  for_each = var.cluster_nodes

  name                = "perf-cluster-public-ip-${each.key}-${var.uuid}"
  location            = "East US"
  resource_group_name = "perf-resources-eastus"
  allocation_method   = "Static"

  tags = {
    role       = "cluster"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Create public client ip(s).
resource "azurerm_public_ip" "perf-public-client" {
  for_each = var.client_nodes

  name                = "perf-client-public-ip-${each.key}-${var.uuid}"
  location            = "East US"
  resource_group_name = "perf-resources-eastus"
  allocation_method   = "Static"

  tags = {
    role       = "client"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Create public utility ip(s).
resource "azurerm_public_ip" "perf-public-utility" {
  for_each = var.utility_nodes

  name                = "perf-utility-public-ip-${each.key}-${var.uuid}"
  location            = "East US"
  resource_group_name = "perf-resources-eastus"
  allocation_method   = "Static"

  tags = {
    role       = "utility"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Create public utility ip(s).
resource "azurerm_public_ip" "perf-public-syncgateway" {
  for_each = var.syncgateway_nodes

  name                = "perf-syncgateway-public-ip-${each.key}-${var.uuid}"
  location            = "East US"
  resource_group_name = "perf-resources-eastus"
  allocation_method   = "Static"

  tags = {
    role       = "sync_gateway"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Create network interface, map public ips.
resource "azurerm_network_interface" "perf-cluster-ni" {
  for_each = var.cluster_nodes

  name                          = "perf-cluster-ni${each.key}-${var.uuid}"
  location                      = "East US"
  resource_group_name           = "perf-resources-eastus"
  enable_accelerated_networking = true

  ip_configuration {
    name                          = "perf-cluster-private-ip-${each.key}-${var.uuid}"
    subnet_id                     = azurerm_subnet.perf-sn.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.perf-public-cluster[each.key].id
  }

  tags = {
    role       = "cluster"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Create network interface, map public ips.
resource "azurerm_network_interface" "perf-client-ni" {
  for_each = var.client_nodes

  name                          = "perf-client-ni${each.key}-${var.uuid}"
  location                      = "East US"
  resource_group_name           = "perf-resources-eastus"
  enable_accelerated_networking = true

  ip_configuration {
    name                          = "perf-client-private-ip-${each.key}-${var.uuid}"
    subnet_id                     = azurerm_subnet.perf-sn.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.perf-public-client[each.key].id
  }

  tags = {
    role       = "client"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Create network interface, map public ips.
resource "azurerm_network_interface" "perf-utility-ni" {
  for_each = var.utility_nodes

  name                          = "perf-utility-ni${each.key}-${var.uuid}"
  location                      = "East US"
  resource_group_name           = "perf-resources-eastus"
  enable_accelerated_networking = true

  ip_configuration {
    name                          = "perf-utility-private-ip-${each.key}-${var.uuid}"
    subnet_id                     = azurerm_subnet.perf-sn.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.perf-public-utility[each.key].id
  }

  tags = {
    role       = "utility"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Create network interface, map public ips.
resource "azurerm_network_interface" "perf-syncgateway-ni" {
  for_each = var.syncgateway_nodes

  name                          = "perf-syncgateway-ni${each.key}-${var.uuid}"
  location                      = "East US"
  resource_group_name           = "perf-resources-eastus"
  enable_accelerated_networking = true

  ip_configuration {
    name                          = "perf-syncgateway-private-ip-${each.key}-${var.uuid}"
    subnet_id                     = azurerm_subnet.perf-sn.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.perf-public-syncgateway[each.key].id
  }

  tags = {
    role       = "sync_gateway"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Config and create cluster disk(s).
resource "azurerm_managed_disk" "perf-cluster-disk" {
  for_each = var.cluster_nodes

  name                 = "perf-cluster-disk-${each.key}-${var.uuid}"
  location             = "East US"
  resource_group_name  = "perf-resources-eastus"
  storage_account_type = each.value.storage_class
  create_option        = "Empty"
  disk_size_gb         = each.value.volume_size
  tier                 = each.value.disk_tier != "" ? each.value.disk_tier : null

  tags = {
    role       = "cluster"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Config and create client disk(s).
resource "azurerm_managed_disk" "perf-client-disk" {
  for_each = var.client_nodes

  name                 = "perf-client-disk-${each.key}-${var.uuid}"
  location             = "East US"
  resource_group_name  = "perf-resources-eastus"
  storage_account_type = each.value.storage_class
  create_option        = "Empty"
  disk_size_gb         = each.value.volume_size
  tier                 = each.value.disk_tier != "" ? each.value.disk_tier : null

  tags = {
    role       = "client"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Config and create utility disk(s).
resource "azurerm_managed_disk" "perf-utility-disk" {
  for_each = var.utility_nodes

  name                 = "perf-utility-disk-${each.key}-${var.uuid}"
  location             = "East US"
  resource_group_name  = "perf-resources-eastus"
  storage_account_type = each.value.storage_class
  create_option        = "Empty"
  disk_size_gb         = each.value.volume_size
  tier                 = each.value.disk_tier != "" ? each.value.disk_tier : null

  tags = {
    role       = "utility"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Config and create sync gateway disk(s).
resource "azurerm_managed_disk" "perf-syncgateway-disk" {
  for_each = var.syncgateway_nodes

  name                 = "perf-syncgateway-disk-${each.key}-${var.uuid}"
  location             = "East US"
  resource_group_name  = "perf-resources-eastus"
  storage_account_type = each.value.storage_class
  create_option        = "Empty"
  disk_size_gb         = each.value.volume_size
  tier                 = each.value.disk_tier != "" ? each.value.disk_tier : null

  tags = {
    role       = "sync_gateway"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Create cluster VMs.
resource "azurerm_virtual_machine" "perf-cluster-vm" {
  for_each = var.cluster_nodes

  name                  = "perf-cluster-vm-${each.key}-${var.uuid}"
  location              = "East US"
  resource_group_name   = "perf-resources-eastus"
  network_interface_ids = [azurerm_network_interface.perf-cluster-ni[each.key].id]
  vm_size               = each.value.instance_type

  storage_image_reference {
    id = each.value.image
  }

  identity {
    type = "UserAssigned"
    identity_ids = ["/subscriptions/a5c0936c-5cec-4c8c-85e1-97f5cab644d9/resourceGroups/perf-resources-eastus/providers/Microsoft.ManagedIdentity/userAssignedIdentities/perfrunner-mi"]
  }

  storage_os_disk {
    name                 = "perf-cluster-os-disk-${each.key}-${var.uuid}"
    caching              = "ReadWrite"
    managed_disk_type    = "Premium_LRS"
    create_option        = "FromImage"
  }

  storage_data_disk {
    name            = azurerm_managed_disk.perf-cluster-disk[each.key].name
    managed_disk_id = azurerm_managed_disk.perf-cluster-disk[each.key].id
    create_option   = "Attach"
    lun             = 1
    disk_size_gb    = azurerm_managed_disk.perf-cluster-disk[each.key].disk_size_gb
    caching         = "ReadWrite"
  }

  delete_os_disk_on_termination    = true
  delete_data_disks_on_termination = true

  tags = {
    role       = "cluster"
    node_group = each.value.node_group
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Create client VM(s).
resource "azurerm_virtual_machine" "perf-client-vm" {
  for_each = var.client_nodes

  name                  = "perf-client-vm-${each.key}-${var.uuid}"
  location              = "East US"
  resource_group_name   = "perf-resources-eastus"
  network_interface_ids = [azurerm_network_interface.perf-client-ni[each.key].id]
  vm_size               = each.value.instance_type

  storage_image_reference {
    id = each.value.image
  }

  identity {
    type = "UserAssigned"
    identity_ids = ["/subscriptions/a5c0936c-5cec-4c8c-85e1-97f5cab644d9/resourceGroups/perf-resources-eastus/providers/Microsoft.ManagedIdentity/userAssignedIdentities/perfrunner-mi"]
  }

  storage_os_disk {
    name                 = "perf-client-os-disk-${each.key}-${var.uuid}"
    caching              = "ReadWrite"
    managed_disk_type    = "Premium_LRS"
    create_option        = "FromImage"
  }

  storage_data_disk {
    name            = azurerm_managed_disk.perf-client-disk[each.key].name
    managed_disk_id = azurerm_managed_disk.perf-client-disk[each.key].id
    create_option   = "Attach"
    lun             = 1
    disk_size_gb    = azurerm_managed_disk.perf-client-disk[each.key].disk_size_gb
    caching         = "ReadWrite"
  }

  delete_os_disk_on_termination    = true
  delete_data_disks_on_termination = true

  tags = {
    role       = "client"
    node_group = each.value.node_group
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Create utility VM(s).
resource "azurerm_virtual_machine" "perf-utility-vm" {
  for_each = var.utility_nodes

  name                  = "perf-utility-vm-${each.key}-${var.uuid}"
  location              = "East US"
  resource_group_name   = "perf-resources-eastus"
  network_interface_ids = [azurerm_network_interface.perf-utility-ni[each.key].id]
  vm_size               = each.value.instance_type

  storage_image_reference {
    id = each.value.image
  }

  storage_os_disk {
    name                 = "perf-utility-os-disk-${each.key}-${var.uuid}"
    caching              = "ReadWrite"
    managed_disk_type    = "Premium_LRS"
    create_option        = "FromImage"
  }

  storage_data_disk {
    name            = azurerm_managed_disk.perf-utility-disk[each.key].name
    managed_disk_id = azurerm_managed_disk.perf-utility-disk[each.key].id
    create_option   = "Attach"
    lun             = 1
    disk_size_gb    = azurerm_managed_disk.perf-utility-disk[each.key].disk_size_gb
    caching         = "ReadWrite"
  }

  delete_os_disk_on_termination    = true
  delete_data_disks_on_termination = true

  tags = {
    role       = "utility"
    node_group = each.value.node_group
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Create sync gateway VM(s).
resource "azurerm_virtual_machine" "perf-syncgateway-vm" {
  for_each = var.syncgateway_nodes

  name                  = "perf-syncgateway-vm-${each.key}-${var.uuid}"
  location              = "East US"
  resource_group_name   = "perf-resources-eastus"
  network_interface_ids = [azurerm_network_interface.perf-syncgateway-ni[each.key].id]
  vm_size               = each.value.instance_type

  storage_image_reference {
    id = each.value.image
  }

  storage_os_disk {
    name                 = "perf-syncgateway-os-disk-${each.key}-${var.uuid}"
    caching              = "ReadWrite"
    managed_disk_type    = "Premium_LRS"
    create_option        = "FromImage"
  }

  storage_data_disk {
    name            = azurerm_managed_disk.perf-syncgateway-disk[each.key].name
    managed_disk_id = azurerm_managed_disk.perf-syncgateway-disk[each.key].id
    create_option   = "Attach"
    lun             = 1
    disk_size_gb    = azurerm_managed_disk.perf-syncgateway-disk[each.key].disk_size_gb
    caching         = "ReadWrite"
  }

  delete_os_disk_on_termination    = true
  delete_data_disks_on_termination = true

  tags = {
    role       = "syncgateway"
    node_group = each.value.node_group
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

resource "azurerm_storage_account" "perf-storage-acc" {
  count                    = var.cloud_storage ? 1 : 0
  name                     = "perfstorageacc${var.uuid}"
  resource_group_name      = "perf-resources-eastus"
  location                 = "East US"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  access_tier              = "Cool"
  tags = {
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

resource "azurerm_storage_container" "perf-storage-container" {
  count                 = var.cloud_storage ? 1 : 0
  name                  = "content${var.uuid}"
  storage_account_name  = element(azurerm_storage_account.perf-storage-acc.*.name, count.index)
  container_access_type = "private"
}
