# # gcs.tf
# resource "google_storage_bucket" "raw" {
#   name          = "${var.project_id}-riskplatform-raw"
#   location      = var.gcs_location
#   force_destroy = true

#   uniform_bucket_level_access = true

#   lifecycle_rule {
#     condition { age = 90 }
#     action { type = "Delete" }
#   }
# }

# resource "google_storage_bucket" "scored" {
#   name          = "${var.project_id}-riskplatform-scored"
#   location      = var.gcs_location
#   force_destroy = true

#   uniform_bucket_level_access = true
# }

# resource "google_storage_bucket" "models" {
#   name          = "${var.project_id}-riskplatform-models"
#   location      = var.gcs_location
#   force_destroy = true

#   uniform_bucket_level_access = true

#   versioning {
#     enabled = true
#   }
# }

# resource "google_storage_bucket" "tf_state" {
#   name          = "${var.project_id}-tf-state"
#   location      = var.gcs_location
#   force_destroy = false

#   uniform_bucket_level_access = true

#   versioning {
#     enabled = true
#   }
# }