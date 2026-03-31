# bigquery.tf
# resource "google_bigquery_dataset" "risk_results" {
#   dataset_id    = "risk_results"
#   friendly_name = "Risk Model Results"
#   description   = "Scored outputs from risk models"
#   location      = var.region

#   delete_contents_on_destroy = true
# }

resource "google_bigquery_table" "scored_accounts" {
  dataset_id          = "risk_results"
  table_id            = "scored_accounts"
  deletion_protection = false

  schema = jsonencode([
    { name = "account_id", type = "STRING", mode = "NULLABLE" },
    { name = "score", type = "FLOAT64", mode = "NULLABLE" },
    { name = "risk_band", type = "STRING", mode = "NULLABLE" },
    { name = "scored_at", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "model_version", type = "STRING", mode = "NULLABLE" },
    { name = "delinquency_risk_score", type = "FLOAT64", mode = "NULLABLE" },
    { name = "debt_stress", type = "INTEGER", mode = "NULLABLE" },
    { name = "high_risk_flag", type = "INTEGER", mode = "NULLABLE" },
    { name = "credit_score", type = "INTEGER", mode = "NULLABLE" },
    { name = "debt_ratio", type = "FLOAT64", mode = "NULLABLE" },
    { name = "monthly_income", type = "INTEGER", mode = "NULLABLE" }
  ])

  time_partitioning {
    type  = "DAY"
    field = "scored_at"
  }
}