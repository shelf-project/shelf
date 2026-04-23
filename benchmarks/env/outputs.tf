output "cluster_name" {
  description = "EKS cluster name. Pass to kubectl / helm."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  value     = module.eks.cluster_endpoint
  sensitive = true
}

output "region" {
  value = var.region
}

output "kubeconfig_command" {
  description = "Paste this to wire kubectl."
  value       = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.region}"
}

output "results_bucket" {
  value = local.results_bucket_name
}

output "vpc_id" {
  value = module.vpc.vpc_id
}

// Structured snapshot of the cluster shape, embedded in every result
// record under the `cluster_shape` field.
output "cluster_shape" {
  description = "Emitted into every result JSON for reproducibility (README §contract)."
  value = {
    region                = var.region
    k8s_version           = var.k8s_version
    trino_instance_type   = var.trino_worker_instance_type
    trino_worker_count    = var.trino_worker_count
    shelf_instance_type   = var.shelf_node_instance_type
    shelf_node_count      = var.shelf_node_count
    driver_instance_type  = var.driver_instance_type
  }
}
