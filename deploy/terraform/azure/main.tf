# Lakeforge on Azure: resource group + VNet + AKS + PostgreSQL Flexible Server
# + ADLS Gen2 storage account (workspace root) + Workload Identity, then Helm.

terraform {
  required_version = ">= 1.5"
  required_providers {
    azurerm    = { source = "hashicorp/azurerm", version = "~> 4.0" }
    azuread    = { source = "hashicorp/azuread", version = "~> 3.0" }
    helm       = { source = "hashicorp/helm", version = "~> 2.14" }
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.31" }
    random     = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
  subscription_id = var.subscription_id
}

provider "azuread" {}

locals {
  name = var.name
  tags = merge({ project = "lakeforge", managed-by = "terraform" }, var.tags)
  # storage account names: 3-24 lowercase alphanumerics
  storage_account = substr(replace("${local.name}ws${random_id.suffix.hex}", "-", ""), 0, 24)
}

resource "random_id" "suffix" {
  byte_length = 3
}

resource "azurerm_resource_group" "rg" {
  name     = "${local.name}-rg"
  location = var.location
  tags     = local.tags
}

# ----------------------------------------------------------------- network --
resource "azurerm_virtual_network" "vnet" {
  name                = local.name
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  address_space       = ["10.42.0.0/16"]
  tags                = local.tags
}

resource "azurerm_subnet" "aks" {
  name                 = "aks"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.42.0.0/18"]
}

resource "azurerm_subnet" "db" {
  name                 = "postgres"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.42.64.0/24"]
  delegation {
    name = "postgres"
    service_delegation {
      name    = "Microsoft.DBforPostgreSQL/flexibleServers"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

resource "azurerm_private_dns_zone" "db" {
  name                = "${local.name}.postgres.database.azure.com"
  resource_group_name = azurerm_resource_group.rg.name
  tags                = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "db" {
  name                  = local.name
  resource_group_name   = azurerm_resource_group.rg.name
  private_dns_zone_name = azurerm_private_dns_zone.db.name
  virtual_network_id    = azurerm_virtual_network.vnet.id
}

# --------------------------------------------------------------------- aks --
resource "azurerm_kubernetes_cluster" "aks" {
  name                = local.name
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  dns_prefix          = local.name
  kubernetes_version  = var.kubernetes_version

  oidc_issuer_enabled       = true
  workload_identity_enabled = true

  default_node_pool {
    name           = "control"
    vm_size        = var.control_plane_vm_size
    node_count     = 1
    vnet_subnet_id = azurerm_subnet.aks.id
    node_labels    = { "lakeforge.io/pool" = "control" }
  }

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    network_plugin = "azure"
    service_cidr   = "10.45.0.0/16"
    dns_service_ip = "10.45.0.10"
  }

  tags = local.tags
}

resource "azurerm_kubernetes_cluster_node_pool" "compute" {
  name                  = "compute"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.aks.id
  vm_size               = var.compute_vm_size
  vnet_subnet_id        = azurerm_subnet.aks.id
  auto_scaling_enabled  = true
  min_count             = var.compute_min_nodes
  max_count             = var.compute_max_nodes
  node_labels           = { "lakeforge.io/pool" = "compute" }
  tags                  = local.tags
}

# ----------------------------------------------------------------- storage --
resource "azurerm_storage_account" "workspace" {
  name                     = local.storage_account
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"
  tags                     = local.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "workspace" {
  name               = "workspace"
  storage_account_id = azurerm_storage_account.workspace.id
}

# Workload identity: managed identities federated to the Kubernetes SAs.
resource "azurerm_user_assigned_identity" "control_plane" {
  name                = "${local.name}-control-plane"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  tags                = local.tags
}

resource "azurerm_user_assigned_identity" "compute" {
  name                = "${local.name}-forge"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  tags                = local.tags
}

resource "azurerm_role_assignment" "control_plane_blob" {
  scope                = azurerm_storage_account.workspace.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.control_plane.principal_id
}

resource "azurerm_role_assignment" "compute_blob" {
  scope                = azurerm_storage_account.workspace.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.compute.principal_id
}

resource "azurerm_federated_identity_credential" "control_plane" {
  name                = "${local.name}-control-plane"
  resource_group_name = azurerm_resource_group.rg.name
  parent_id           = azurerm_user_assigned_identity.control_plane.id
  audience            = ["api://AzureADTokenExchange"]
  issuer              = azurerm_kubernetes_cluster.aks.oidc_issuer_url
  subject             = "system:serviceaccount:${var.namespace}:lakeforge"
}

resource "azurerm_federated_identity_credential" "compute" {
  name                = "${local.name}-forge"
  resource_group_name = azurerm_resource_group.rg.name
  parent_id           = azurerm_user_assigned_identity.compute.id
  audience            = ["api://AzureADTokenExchange"]
  issuer              = azurerm_kubernetes_cluster.aks.oidc_issuer_url
  subject             = "system:serviceaccount:lakeforge-compute:forge"
}

# ---------------------------------------------------------------- database --
resource "random_password" "db" {
  length  = 24
  special = false
}

resource "azurerm_postgresql_flexible_server" "db" {
  name                          = "${local.name}-${random_id.suffix.hex}"
  resource_group_name           = azurerm_resource_group.rg.name
  location                      = azurerm_resource_group.rg.location
  version                       = "16"
  delegated_subnet_id           = azurerm_subnet.db.id
  private_dns_zone_id           = azurerm_private_dns_zone.db.id
  public_network_access_enabled = false
  administrator_login           = "lakeforge"
  administrator_password        = random_password.db.result
  sku_name                      = var.db_sku
  storage_mb                    = 65536
  backup_retention_days         = 7
  zone                          = "1"
  tags                          = local.tags

  depends_on = [azurerm_private_dns_zone_virtual_network_link.db]
}

resource "azurerm_postgresql_flexible_server_database" "lakeforge" {
  name      = "lakeforge"
  server_id = azurerm_postgresql_flexible_server.db.id
}

# -------------------------------------------------------------------- helm --
provider "kubernetes" {
  host                   = azurerm_kubernetes_cluster.aks.kube_config[0].host
  client_certificate     = base64decode(azurerm_kubernetes_cluster.aks.kube_config[0].client_certificate)
  client_key             = base64decode(azurerm_kubernetes_cluster.aks.kube_config[0].client_key)
  cluster_ca_certificate = base64decode(azurerm_kubernetes_cluster.aks.kube_config[0].cluster_ca_certificate)
}

provider "helm" {
  kubernetes {
    host                   = azurerm_kubernetes_cluster.aks.kube_config[0].host
    client_certificate     = base64decode(azurerm_kubernetes_cluster.aks.kube_config[0].client_certificate)
    client_key             = base64decode(azurerm_kubernetes_cluster.aks.kube_config[0].client_key)
    cluster_ca_certificate = base64decode(azurerm_kubernetes_cluster.aks.kube_config[0].cluster_ca_certificate)
  }
}

module "lakeforge" {
  source = "../modules/lakeforge"

  namespace      = var.namespace
  cloud          = "azure"
  api_image      = var.api_image
  forge_image    = var.forge_image
  image_tag      = var.image_tag
  admin_user     = var.admin_user
  admin_password = var.admin_password
  public_url     = var.public_url

  database_url = "postgres://lakeforge:${random_password.db.result}@${azurerm_postgresql_flexible_server.db.fqdn}:5432/lakeforge?sslmode=require"
  storage_root = "az://${azurerm_storage_data_lake_gen2_filesystem.workspace.name}/workspace"
  # The Azure workload-identity webhook injects AZURE_CLIENT_ID / AZURE_TENANT_ID /
  # AZURE_FEDERATED_TOKEN_FILE into pods labelled azure.workload.identity/use=true.
  storage_env = {
    AZURE_STORAGE_ACCOUNT_NAME = azurerm_storage_account.workspace.name
  }

  control_plane_sa_annotations = { "azure.workload.identity/client-id" = azurerm_user_assigned_identity.control_plane.client_id }
  compute_sa_annotations       = { "azure.workload.identity/client-id" = azurerm_user_assigned_identity.compute.client_id }

  service_type = "LoadBalancer"
  extra_values = merge(
    {
      podLabels = { "azure.workload.identity/use" = "true" }
      forge = {
        podLabels    = { "azure.workload.identity/use" = "true" }
        nodeSelector = { "lakeforge.io/pool" = "compute" }
      }
    },
    var.extra_helm_values,
  )

  depends_on = [azurerm_kubernetes_cluster_node_pool.compute]
}
