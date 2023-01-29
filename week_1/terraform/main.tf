locals {
  project_name = "de-zoomcamp-376020"
  region       = "australia-southeast1"

  data_lake_bucket_name = "${var.BQ_DATASET}_${local.project_name}_bucket"
}

# Data Lake
resource "google_storage_bucket" "data-lake-bucket" {
  name     = "${local.data_lake_bucket_name}_${local.project_name}"
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

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = local.project_name
  location   = local.region
}
