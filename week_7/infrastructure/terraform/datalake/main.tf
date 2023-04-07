locals {
  project_name = var.project_name
  region       = var.project_region

  bucket_name  = var.datalake_name
  dataset_name = var.dataset_name
}

resource "google_storage_bucket" "data-lake-bucket" {
  name     = "${local.project_name}_${local.bucket_name}"
  location = local.region

  storage_class = "STANDARD"

  force_destroy = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # in days
    }
  }
}

# resource "google_bigquery_dataset" "dataset" {
#   dataset_id = local.dataset_name
#   project    = local.project_name
#   location   = local.region
# }
