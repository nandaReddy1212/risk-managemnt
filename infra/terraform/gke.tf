#gke
resource "google_container_cluster" "riskplatform" {
  name     = var.cluster_name
  location = var.zone

  remove_default_node_pool = true
  initial_node_count       = 1

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  ip_allocation_policy {}

  release_channel {
    channel = "REGULAR"
  }

  deletion_protection = false

  timeouts {
    create = "20m"
    update = "20m"
    delete = "20m"
  }
}

resource "google_container_node_pool" "primary" {
  name     = "primary-pool"
  location = var.zone
  cluster  = google_container_cluster.riskplatform.name

  autoscaling {
    min_node_count = 2
    max_node_count = 4
  }

  initial_node_count = 2

  node_config {
    machine_type = "e2-standard-2"
    disk_type    = "pd-standard" # HDD not SSD
    disk_size_gb = 50            # 50GB not 100GB

    service_account = data.google_service_account.terraform_runner.email

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
}
