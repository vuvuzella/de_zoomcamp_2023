locals {
  project_name = "de-zoomcamp-376020"
  region       = "australia-southeast1"

  bucket_name = "week_3_data_store"
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
#   dataset_id = "de_zoomcamp_dataset"
#   project    = local.project_name
#   location   = local.region
# }

# resource "google_bigquery_table" "data_warehouse" {
#   dataset_id = google_bigquery_dataset.dataset.dataset_id
#   table_id   = "yellow_taxi_data"
#   project    = local.project_name
#   schema     = file("./yellow_tripdata_2021-01.json")
# 
#   # external_data_configuration {
#   #   autodetect = true
#   #   source_format = "PARQUET"
#   #   source_uris = [
#   #     "gs://de-zoomcamp-376020_week_2_data_store/data/yellow_tripdata_2021-01.parquet"
#   #   ]
#   # }
# 
#   deletion_protection = false
# 
# }
