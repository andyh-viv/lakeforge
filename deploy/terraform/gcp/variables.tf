variable "project" {
  type = string
}
variable "region" {
  type    = string
  default = "us-central1"
}
variable "name" {
  type    = string
  default = "lakeforge"
}
variable "control_plane_machine_type" {
  type    = string
  default = "e2-standard-2"
}
variable "compute_machine_type" {
  type    = string
  default = "e2-standard-8"
}
variable "compute_min_nodes" {
  type    = number
  default = 1
}
variable "compute_max_nodes" {
  type    = number
  default = 10
}
variable "db_tier" {
  type    = string
  default = "db-custom-2-7680"
}
variable "db_high_availability" {
  type    = bool
  default = false
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
variable "labels" {
  type    = map(string)
  default = {}
}
