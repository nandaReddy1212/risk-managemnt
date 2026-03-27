# # outputs.tf
# output "cluster_name" {
#   value = google_container_cluster.riskplatform.name
# }

# output "cluster_endpoint" {
#   value     = google_container_cluster.riskplatform.endpoint
#   sensitive = true
# }

output "gcs_raw_bucket" {
  value = google_storage_bucket.raw.name
}

output "gcs_scored_bucket" {
  value = google_storage_bucket.scored.name
}

output "gcs_models_bucket" {
  value = google_storage_bucket.models.name
}

output "bq_dataset" {
  value = google_bigquery_dataset.risk_results.dataset_id
}

output "service_account_email" {
  value = google_service_account.riskplatform_sa.email
}