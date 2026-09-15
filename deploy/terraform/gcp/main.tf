# Lakeforge on GCP: VPC + GKE + Cloud SQL Postgres + GCS (workspace root) +
# Workload Identity, then the Helm chart.

terraform {
  required_version = ">= 1.5"
  required_providers {
    google     = { source = "hashicorp/google", version = "~> 6.0" }
    helm       = { source = "hashicorp/helm", version = "~> 2.14" }
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.31" }
    random     = { source = "hashicorp/random", version = "~> 3.6" }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

locals {
  name   = var.name
  labels = merge({ project = "lakeforge", managed-by = "terraform" }, var.labels)
  apis = [
    "container.googleapis.com",
    "compute.googleapis.com",
    "sqladmin.googleapis.com",
    "servicenetworking.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
  ]
}

resource "google_project_service" "apis" {
  for_each           = toset(local.apis)
  service            = each.value
  disable_on_destroy = false
}

# ----------------------------------------------------------------- network --
resource "google_compute_network" "vpc" {
  name                    = local.name
  auto_create_subnetworks = false
  depends_on              = [google_project_service.apis]
}

resource "google_compute_subnetwork" "gke" {
  name                     = "${local.name}-gke"
  network                  = google_compute_network.vpc.id
  region                   = var.region
  ip_cidr_range            = "10.42.0.0/20"
  private_ip_google_access = true
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.44.0.0/16"
  }
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.45.0.0/20"
  }
}

resource "google_compute_router" "router" {
  name    = local.name
  network = google_compute_network.vpc.id
  region  = var.region
}

resource "google_compute_router_nat" "nat" {
  name                               = local.name
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# Private services access for Cloud SQL.
resource "google_compute_global_address" "psa" {
  name          = "${local.name}-psa"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.vpc.id
}

resource "google_service_networking_connection" "psa" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.psa.name]
  depends_on              = [google_project_service.apis]
}

# --------------------------------------------------------------------- gke --
resource "google_container_cluster" "gke" {
  name     = local.name
  location = var.region

  network    = google_compute_network.vpc.id
  subnetwork = google_compute_subnetwork.gke.id

  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = !var.force_destroy_storage

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  workload_identity_config {
    workload_pool = "${var.project}.svc.id.goog"
  }

  release_channel {
    channel = "REGULAR"
  }

  resource_labels = local.labels
  depends_on      = [google_project_service.apis]
}

resource "google_container_node_pool" "control" {
  name       = "control"
  cluster    = google_container_cluster.gke.id
  node_count = 1
  node_config {
    machine_type    = var.control_plane_machine_type
    labels          = { "lakeforge.io/pool" = "control" }
    service_account = google_service_account.nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
}

resource "google_container_node_pool" "compute" {
  name    = "compute"
  cluster = google_container_cluster.gke.id
  autoscaling {
    min_node_count = var.compute_min_nodes
    max_node_count = var.compute_max_nodes
  }
  initial_node_count = var.compute_min_nodes
  node_config {
    machine_type    = var.compute_machine_type
    labels          = { "lakeforge.io/pool" = "compute" }
    service_account = google_service_account.nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
}

resource "google_service_account" "nodes" {
  account_id   = "${local.name}-nodes"
  display_name = "Lakeforge GKE nodes"
}

resource "google_project_iam_member" "nodes" {
  for_each = toset(["roles/logging.logWriter", "roles/monitoring.metricWriter", "roles/artifactregistry.reader"])
  project  = var.project
  role     = each.value
  member   = "serviceAccount:${google_service_account.nodes.email}"
}

# ----------------------------------------------------------------- storage --
resource "google_storage_bucket" "workspace" {
  name                        = "${local.name}-workspace-${var.project}"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = var.force_destroy_storage
  versioning {
    enabled = true
  }
  labels = local.labels
}

# Workload Identity: control-plane and compute Kubernetes SAs -> GCP SAs with bucket access.
resource "google_service_account" "control_plane" {
  account_id   = "${local.name}-control-plane"
  display_name = "Lakeforge control plane"
}

resource "google_service_account" "compute" {
  account_id   = "${local.name}-forge"
  display_name = "Lakeforge Forge compute"
}

resource "google_storage_bucket_iam_member" "control_plane" {
  bucket = google_storage_bucket.workspace.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.control_plane.email}"
}

resource "google_storage_bucket_iam_member" "compute" {
  bucket = google_storage_bucket.workspace.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.compute.email}"
}

resource "google_service_account_iam_member" "control_plane_wi" {
  service_account_id = google_service_account.control_plane.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project}.svc.id.goog[${var.namespace}/lakeforge]"
}

resource "google_service_account_iam_member" "compute_wi" {
  service_account_id = google_service_account.compute.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project}.svc.id.goog[lakeforge-compute/forge]"
}

# ---------------------------------------------------------------- database --
resource "random_password" "db" {
  length  = 24
  special = false
}

resource "google_sql_database_instance" "db" {
  name                = "${local.name}-${random_id.db_suffix.hex}"
  database_version    = "POSTGRES_16"
  region              = var.region
  deletion_protection = !var.force_destroy_storage

  settings {
    tier              = var.db_tier
    availability_type = var.db_high_availability ? "REGIONAL" : "ZONAL"
    disk_autoresize   = true
    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.vpc.id
    }
    backup_configuration {
      enabled = true
    }
    user_labels = local.labels
  }
  depends_on = [google_service_networking_connection.psa]
}

resource "random_id" "db_suffix" {
  byte_length = 3
}

resource "google_sql_database" "lakeforge" {
  name     = "lakeforge"
  instance = google_sql_database_instance.db.name
}

resource "google_sql_user" "lakeforge" {
  name     = "lakeforge"
  instance = google_sql_database_instance.db.name
  password = random_password.db.result
}

# -------------------------------------------------------------------- helm --
data "google_client_config" "current" {}

provider "kubernetes" {
  host                   = "https://${google_container_cluster.gke.endpoint}"
  cluster_ca_certificate = base64decode(google_container_cluster.gke.master_auth[0].cluster_ca_certificate)
  token                  = data.google_client_config.current.access_token
}

provider "helm" {
  kubernetes {
    host                   = "https://${google_container_cluster.gke.endpoint}"
    cluster_ca_certificate = base64decode(google_container_cluster.gke.master_auth[0].cluster_ca_certificate)
    token                  = data.google_client_config.current.access_token
  }
}

module "lakeforge" {
  source = "../modules/lakeforge"

  namespace      = var.namespace
  cloud          = "gcp"
  api_image      = var.api_image
  forge_image    = var.forge_image
  image_tag      = var.image_tag
  admin_user     = var.admin_user
  admin_password = var.admin_password
  public_url     = var.public_url

  database_url = "postgres://lakeforge:${random_password.db.result}@${google_sql_database_instance.db.private_ip_address}:5432/lakeforge"
  storage_root = "gs://${google_storage_bucket.workspace.name}/workspace"

  control_plane_sa_annotations = { "iam.gke.io/gcp-service-account" = google_service_account.control_plane.email }
  compute_sa_annotations       = { "iam.gke.io/gcp-service-account" = google_service_account.compute.email }

  service_type = "LoadBalancer"
  extra_values = merge({ forge = { nodeSelector = { "lakeforge.io/pool" = "compute" } } }, var.extra_helm_values)

  depends_on = [google_container_node_pool.control, google_container_node_pool.compute]
}
