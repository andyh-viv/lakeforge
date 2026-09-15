output "workspace_url" {
  value = coalesce(var.public_url, module.lakeforge.load_balancer_address != "" ? "http://${module.lakeforge.load_balancer_address}" : "")
}
output "admin_user" {
  value = var.admin_user
}
output "admin_password_command" {
  value = "kubectl -n ${var.namespace} get secret lakeforge-secrets -o jsonpath='{.data.admin-password}' | base64 -d"
}
output "kubeconfig_command" {
  value = "gcloud container clusters get-credentials ${google_container_cluster.gke.name} --region ${var.region} --project ${var.project}"
}
output "workspace_bucket" {
  value = google_storage_bucket.workspace.name
}
output "database_private_ip" {
  value = google_sql_database_instance.db.private_ip_address
}
