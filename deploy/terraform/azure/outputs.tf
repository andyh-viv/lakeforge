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
  value = "az aks get-credentials --resource-group ${azurerm_resource_group.rg.name} --name ${azurerm_kubernetes_cluster.aks.name}"
}
output "storage_account" {
  value = azurerm_storage_account.workspace.name
}
output "database_fqdn" {
  value = azurerm_postgresql_flexible_server.db.fqdn
}
