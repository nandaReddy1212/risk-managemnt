# iam.tf

# Reference your EXISTING service account (don't create a new one)
data "google_service_account" "terraform_runner" {
  account_id = "terraform-runner"
  project    = var.project_id
}

# Keep riskplatform-sa for workload identity (this one we still create)
resource "google_service_account" "riskplatform_sa" {
  account_id   = "riskplatform-sa"
  display_name = "RiskPlatform Workload Identity SA"
}

resource "google_project_iam_member" "sa_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.riskplatform_sa.email}"
}

resource "google_project_iam_member" "sa_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.riskplatform_sa.email}"
}

resource "google_project_iam_member" "sa_bq_job" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.riskplatform_sa.email}"
}

resource "google_service_account_iam_member" "workload_identity_binding" {
  service_account_id = google_service_account.riskplatform_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[riskplatform/riskplatform-ksa]"
}