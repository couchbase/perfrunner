terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.116"
    }
  }
}

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
    disk_tier     = string
  }))
}

variable "cloud_storage" {
  type = bool
}

variable "global_tag" {
  type = string
}

variable "managed_id" {
  type = string
}

variable "allowed_ips" {
  type = list(string)
}

data "azurerm_shared_image_version" "cluster-image" {
  for_each = var.cluster_nodes

  name                = "latest"
  image_name          = each.value.image
  gallery_name        = "perf_vm_images"
  resource_group_name = "perf-resources-eastus"
}

data "azurerm_shared_image_version" "client-image" {
  for_each = var.client_nodes

  name                = "latest"
  image_name          = each.value.image
  gallery_name        = "perf_vm_images"
  resource_group_name = "perf-resources-eastus"
}

data "azurerm_shared_image_version" "utility-image" {
  for_each = var.utility_nodes

  name                = "latest"
  image_name          = each.value.image
  gallery_name        = "perf_vm_images"
  resource_group_name = "perf-resources-eastus"
}

data "azurerm_shared_image_version" "syncgateway-image" {
  for_each = var.syncgateway_nodes

  name                = "latest"
  image_name          = each.value.image
  gallery_name        = "perf_vm_images"
  resource_group_name = "perf-resources-eastus"
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

# Create public syncgateway ip(s).
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
  accelerated_networking_enabled = true

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
  accelerated_networking_enabled = true

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
  accelerated_networking_enabled = true

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
  accelerated_networking_enabled = true

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

resource "azurerm_application_security_group" "cluster-asg" {
  name                = "perf-cluster-asg-${var.uuid}"
  location            = "East US"
  resource_group_name = "perf-resources-eastus"

  tags = {
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

resource "azurerm_application_security_group" "utility-asg" {
  name                = "perf-utility-asg-${var.uuid}"
  location            = "East US"
  resource_group_name = "perf-resources-eastus"

  tags = {
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

resource "azurerm_application_security_group" "syncgateway-asg" {
  name                = "perf-syncgateway-asg-${var.uuid}"
  location            = "East US"
  resource_group_name = "perf-resources-eastus"

  tags = {
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

resource "azurerm_network_interface_application_security_group_association" "cluster-asg-association" {
  for_each = var.cluster_nodes

  network_interface_id          = azurerm_network_interface.perf-cluster-ni[each.key].id
  application_security_group_id = azurerm_application_security_group.cluster-asg.id

  depends_on = [azurerm_virtual_machine.perf-cluster-vm]
}

resource "azurerm_network_interface_application_security_group_association" "utility-asg-association" {
  for_each = var.utility_nodes

  network_interface_id          = azurerm_network_interface.perf-utility-ni[each.key].id
  application_security_group_id = azurerm_application_security_group.utility-asg.id

  depends_on = [azurerm_virtual_machine.perf-utility-vm]
}

resource "azurerm_network_interface_application_security_group_association" "syncgateway-asg-association" {
  for_each = var.syncgateway_nodes

  network_interface_id          = azurerm_network_interface.perf-syncgateway-ni[each.key].id
  application_security_group_id = azurerm_application_security_group.syncgateway-asg.id

  depends_on = [azurerm_virtual_machine.perf-syncgateway-vm]
}

locals {
  allowed_external_ips = concat(
    var.allowed_ips,
    [for ip in azurerm_public_ip.perf-public-client: ip.ip_address],
    [for ip in azurerm_public_ip.perf-public-cluster: ip.ip_address],
    [for ip in azurerm_public_ip.perf-public-syncgateway: ip.ip_address]
  )
}

# Create network security group.
resource "azurerm_network_security_group" "perf-nsg" {
  name                = "perf-nsg-${var.uuid}"
  location            = "East US"
  resource_group_name = "perf-resources-eastus"

  security_rule {
    name                       = "perf-nsg-allow-ssh"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefixes    = local.allowed_external_ips
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "perf-nsg-allow-broker-vnet"
    priority                   = 1002
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "5672"
    source_address_prefix      = "VirtualNetwork"
    destination_application_security_group_ids = [azurerm_application_security_group.utility-asg.id]
  }

  security_rule {
    name                       = "perf-nsg-allow-broker-external"
    priority                   = 1003
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "5672"
    source_address_prefixes    = local.allowed_external_ips
    destination_application_security_group_ids = [azurerm_application_security_group.utility-asg.id]
  }

  security_rule {
    name                       = "perf-nsg-allow-couchbase-vnet"
    priority                   = 1004
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_ranges    = ["8091-8096", "9102", "9110", "18091-18096", "19102", "19110", "11209-11210","11207"]
    source_address_prefix      = "VirtualNetwork"
    destination_application_security_group_ids = [azurerm_application_security_group.cluster-asg.id]
  }

  security_rule {
    name                       = "perf-nsg-allow-couchbase-external"
    priority                   = 1005
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_ranges    = ["8091-8096", "9102", "9110", "18091-18096", "19102", "19110", "11209-11210","11207"]
    source_address_prefixes    = local.allowed_external_ips
    destination_application_security_group_ids = [azurerm_application_security_group.cluster-asg.id]
  }

  security_rule {
    name                       = "perf-nsg-allow-syncgateway-vnet"
    priority                   = 1006
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "4984-5025"
    source_address_prefix      = "VirtualNetwork"
    destination_application_security_group_ids = [azurerm_application_security_group.syncgateway-asg.id]
  }

  security_rule {
    name                       = "perf-nsg-allow-syncgateway-external"
    priority                   = 1007
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "4984-5025"
    source_address_prefixes    = local.allowed_external_ips
    destination_application_security_group_ids = [azurerm_application_security_group.syncgateway-asg.id]
  }

  tags = {
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

resource "azurerm_subnet_network_security_group_association" "perf-nsg-subnet-assoc" {
  subnet_id                 = azurerm_subnet.perf-sn.id
  network_security_group_id = azurerm_network_security_group.perf-nsg.id
}

# Config and create cluster OS disks.
resource "azurerm_managed_disk" "perf-cluster-os-disk" {
  for_each = var.cluster_nodes

  name                       = "perf-cluster-os-disk-${each.key}-${var.uuid}"
  location                   = "East US"
  resource_group_name        = "perf-resources-eastus"
  storage_account_type       = "Premium_LRS"
  create_option              = "FromImage"
  gallery_image_reference_id = data.azurerm_shared_image_version.cluster-image[each.key].id

  tags = {
    role       = "cluster"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Config and create client OS disks.
resource "azurerm_managed_disk" "perf-client-os-disk" {
  for_each = var.client_nodes

  name                       = "perf-client-os-disk-${each.key}-${var.uuid}"
  location                   = "East US"
  resource_group_name        = "perf-resources-eastus"
  storage_account_type       = "Premium_LRS"
  create_option              = "FromImage"
  gallery_image_reference_id = data.azurerm_shared_image_version.client-image[each.key].id

  tags = {
    role       = "client"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Config and create utility OS disks.
resource "azurerm_managed_disk" "perf-utility-os-disk" {
  for_each = var.utility_nodes

  name                       = "perf-utility-os-disk-${each.key}-${var.uuid}"
  location                   = "East US"
  resource_group_name        = "perf-resources-eastus"
  storage_account_type       = "Premium_LRS"
  create_option              = "FromImage"
  gallery_image_reference_id = data.azurerm_shared_image_version.utility-image[each.key].id

  tags = {
    role       = "utility"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Config and create syncgateway OS disks.
resource "azurerm_managed_disk" "perf-syncgateway-os-disk" {
  for_each = var.syncgateway_nodes

  name                       = "perf-syncgateway-os-disk-${each.key}-${var.uuid}"
  location                   = "East US"
  resource_group_name        = "perf-resources-eastus"
  storage_account_type       = "Premium_LRS"
  create_option              = "FromImage"
  gallery_image_reference_id = data.azurerm_shared_image_version.syncgateway-image[each.key].id

  tags = {
    role       = "sync_gateway"
    deployment = var.global_tag != "" ? var.global_tag : null
  }
}

# Config and create cluster data disks.
resource "azurerm_managed_disk" "perf-cluster-data-disk" {
  for_each = {for k, node in var.cluster_nodes : k => node if node.volume_size > 0}

  name                 = "perf-cluster-data-disk-${each.key}-${var.uuid}"
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

# Config and create client data disks.
resource "azurerm_managed_disk" "perf-client-data-disk" {
  for_each = {for k, node in var.client_nodes : k => node if node.volume_size > 0}

  name                 = "perf-client-data-disk-${each.key}-${var.uuid}"
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

# Config and create syncgateway data disks.
resource "azurerm_managed_disk" "perf-syncgateway-data-disk" {
  for_each = {for k, node in var.syncgateway_nodes : k => node if node.volume_size > 0}

  name                 = "perf-syncgateway-data-disk-${each.key}-${var.uuid}"
  location             = "East US"
  resource_group_name  = "perf-resources-eastus"
  storage_account_type = each.value.storage_class
  create_option        = "Empty"
  disk_size_gb         = each.value.volume_size
  tier                 = each.value.disk_tier != "" ? each.value.disk_tier : null

  tags = {
    role       = "syncgateway"
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

  identity {
    type = "UserAssigned"
    identity_ids = [var.managed_id]
  }

  storage_os_disk {
    name                 = azurerm_managed_disk.perf-cluster-os-disk[each.key].name
    managed_disk_id      = azurerm_managed_disk.perf-cluster-os-disk[each.key].id
    create_option        = "Attach"
    caching              = "ReadWrite"
    os_type              = "Linux"
  }

  storage_data_disk {
    name            = azurerm_managed_disk.perf-cluster-data-disk[each.key].name
    managed_disk_id = azurerm_managed_disk.perf-cluster-data-disk[each.key].id
    create_option   = "Attach"
    lun             = 1
    disk_size_gb    = azurerm_managed_disk.perf-cluster-data-disk[each.key].disk_size_gb
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

  identity {
    type = "UserAssigned"
    identity_ids = [var.managed_id]
  }

  storage_os_disk {
    name                 = azurerm_managed_disk.perf-client-os-disk[each.key].name
    managed_disk_id      = azurerm_managed_disk.perf-client-os-disk[each.key].id
    create_option        = "Attach"
    caching              = "ReadWrite"
    os_type              = "Linux"
  }

  storage_data_disk {
    name            = azurerm_managed_disk.perf-client-data-disk[each.key].name
    managed_disk_id = azurerm_managed_disk.perf-client-data-disk[each.key].id
    create_option   = "Attach"
    lun             = 1
    disk_size_gb    = azurerm_managed_disk.perf-client-data-disk[each.key].disk_size_gb
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

  storage_os_disk {
    name                 = azurerm_managed_disk.perf-utility-os-disk[each.key].name
    managed_disk_id      = azurerm_managed_disk.perf-utility-os-disk[each.key].id
    create_option        = "Attach"
    caching              = "ReadWrite"
    os_type              = "Linux"
  }

  delete_os_disk_on_termination    = true

  tags = {
    role       = "utility"
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

  storage_os_disk {
    name                 = azurerm_managed_disk.perf-syncgateway-os-disk[each.key].name
    managed_disk_id      = azurerm_managed_disk.perf-syncgateway-os-disk[each.key].id
    create_option        = "Attach"
    caching              = "ReadWrite"
    os_type              = "Linux"
  }

  storage_data_disk {
    name            = azurerm_managed_disk.perf-syncgateway-data-disk[each.key].name
    managed_disk_id = azurerm_managed_disk.perf-syncgateway-data-disk[each.key].id
    create_option   = "Attach"
    lun             = 1
    disk_size_gb    = azurerm_managed_disk.perf-syncgateway-data-disk[each.key].disk_size_gb
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
