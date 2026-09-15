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
  value = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}
output "workspace_bucket" {
  value = aws_s3_bucket.workspace.bucket
}
output "database_endpoint" {
  value = aws_db_instance.db.address
}
