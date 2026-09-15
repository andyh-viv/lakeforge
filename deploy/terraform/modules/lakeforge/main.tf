# Cloud-agnostic: installs the Lakeforge Helm chart onto an existing Kubernetes
# cluster. The caller configures the `helm` and `kubernetes` providers.

terraform {
  required_version = ">= 1.5"
  required_providers {
    helm = {
      source  = "hashicorp/helm"
      version = ">= 2.12, < 4.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.25"
    }
  }
}

variable "namespace" {
  type    = string
  default = "lakeforge"
}
variable "release_name" {
  type    = string
  default = "lakeforge"
}
variable "chart_path" {
  type        = string
  default     = null
  description = "Local path to deploy/helm/lakeforge (defaults to the copy in this repo)."
}
variable "api_image" {
  type    = string
  default = "ghcr.io/andyh-viv/lakeforge-api"
}
variable "forge_image" {
  type    = string
  default = "ghcr.io/andyh-viv/lakeforge-forge"
}
variable "image_tag" {
  type    = string
  default = "latest"
}
variable "cloud" {
  type = string
}
variable "database_url" {
  type      = string
  sensitive = true
}
variable "storage_root" {
  type = string
}
variable "storage_env" {
  type    = map(string)
  default = {}
}
variable "control_plane_sa_annotations" {
  type    = map(string)
  default = {}
}
variable "compute_sa_annotations" {
  type    = map(string)
  default = {}
}
variable "compute_namespace" {
  type    = string
  default = "lakeforge-compute"
}
variable "public_url" {
  type    = string
  default = ""
}
variable "admin_user" {
  type    = string
  default = "admin@lakeforge.local"
}
variable "admin_password" {
  type      = string
  default   = ""
  sensitive = true
}
variable "service_type" {
  type    = string
  default = "LoadBalancer"
}
variable "service_annotations" {
  type    = map(string)
  default = {}
}
variable "ingress" {
  type = object({
    enabled     = bool
    class_name  = optional(string, "")
    host        = optional(string, "")
    annotations = optional(map(string), {})
    tls_secret  = optional(string, "")
  })
  default = { enabled = false }
}
variable "extra_values" {
  type        = any
  default     = {}
  description = "Additional Helm values merged last."
}

resource "kubernetes_namespace_v1" "lakeforge" {
  metadata {
    name = var.namespace
  }
}

locals {
  values = {
    image = { repository = var.api_image, tag = var.image_tag }
    forge = { image = var.forge_image, tag = var.image_tag, namespace = var.compute_namespace }
    controlPlane = {
      publicUrl     = var.public_url
      cloud         = var.cloud
      adminUser     = var.admin_user
      adminPassword = var.admin_password
    }
    database = { url = var.database_url }
    storage = {
      root        = var.storage_root
      env         = var.storage_env
      persistence = { enabled = true, size = "20Gi" }
    }
    serviceAccount = {
      annotations = var.control_plane_sa_annotations
      compute     = { annotations = var.compute_sa_annotations }
    }
    service = { type = var.service_type, annotations = var.service_annotations }
    ingress = {
      enabled     = var.ingress.enabled
      className   = var.ingress.class_name
      host        = var.ingress.host
      annotations = var.ingress.annotations
      tls         = var.ingress.tls_secret != "" ? [{ secretName = var.ingress.tls_secret, hosts = [var.ingress.host] }] : []
    }
  }
}

resource "helm_release" "lakeforge" {
  name      = var.release_name
  namespace = kubernetes_namespace_v1.lakeforge.metadata[0].name
  chart     = coalesce(var.chart_path, "${path.module}/../../../helm/lakeforge")
  timeout   = 600
  wait      = true

  values = [yamlencode(local.values), yamlencode(var.extra_values)]
}

data "kubernetes_service_v1" "api" {
  metadata {
    name      = var.release_name
    namespace = var.namespace
  }
  depends_on = [helm_release.lakeforge]
}

output "namespace" {
  value = var.namespace
}
output "load_balancer_address" {
  value = try(
    coalesce(
      data.kubernetes_service_v1.api.status[0].load_balancer[0].ingress[0].hostname,
      data.kubernetes_service_v1.api.status[0].load_balancer[0].ingress[0].ip,
    ),
    "",
  )
}
output "admin_user" {
  value = var.admin_user
}
