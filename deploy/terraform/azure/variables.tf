variable "subscription_id" {
  type = string
}
variable "location" {
  type    = string
  default = "eastus"
}
variable "name" {
  type    = string
  default = "lakeforge"
}
variable "kubernetes_version" {
  type    = string
  default = null
}
variable "control_plane_vm_size" {
  type    = string
  default = "Standard_D2s_v5"
}
variable "compute_vm_size" {
  type    = string
  default = "Standard_D8s_v5"
}
variable "compute_min_nodes" {
  type    = number
  default = 1
}
variable "compute_max_nodes" {
  type    = number
  default = 10
}
variable "db_sku" {
  type    = string
  default = "GP_Standard_D2ds_v5"
}
variable "force_destroy_storage" {
  type    = bool
  default = false
}
variable "namespace" {
  type    = string
  default = "lakeforge"
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
variable "admin_user" {
  type    = string
  default = "admin@lakeforge.local"
}
variable "admin_password" {
  type      = string
  default   = ""
  sensitive = true
}
variable "public_url" {
  type    = string
  default = ""
}
variable "extra_helm_values" {
  type    = any
  default = {}
}
variable "tags" {
  type    = map(string)
  default = {}
}
