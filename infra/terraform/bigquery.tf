# bigquery.tf
resource "google_bigquery_dataset" "risk_results" {
  dataset_id    = "risk_results"
  friendly_name = "Risk Model Results"
  description   = "Scored outputs from risk models"
  location      = var.region

  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "scored_accounts" {
  dataset_id          = google_bigquery_dataset.risk_results.dataset_id
  table_id            = "scored_accounts"
  deletion_protection = false

  schema = jsonencode([
    { name = "account_id",      type = "STRING",    mode = "REQUIRED" },
    { name = "score",           type = "FLOAT64",   mode = "REQUIRED" },
    { name = "risk_band",       type = "STRING",    mode = "REQUIRED" },
    { name = "scored_at",       type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "model_version",   type = "STRING",    mode = "NULLABLE" }
  ])

  time_partitioning {
    type  = "DAY"
    field = "scored_at"
  }
}