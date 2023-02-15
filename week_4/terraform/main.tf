locals {
  project_name = "de-zoomcamp-376020"
  region       = "australia-southeast1"

  # bucket_name = "week_4_data_store"
}

# resource "google_storage_bucket" "data-lake-bucket" {
#   name     = "${local.project_name}_${local.bucket_name}"
#   location = local.region
# 
#   storage_class = "STANDARD"
# 
#   force_destroy = true
# 
#   versioning {
#     enabled = true
#   }
# 
#   lifecycle_rule {
#     action {
#       type = "Delete"
#     }
#     condition {
#       age = 30 # in days
#     }
#   }
# }

# resource "google_bigquery_dataset" "dataset" {
#   dataset_id = "de_zoomcamp_dataset"
#   project    = local.project_name
#   location   = local.region
# }


resource "google_bigquery_dataset" "dataset" {
  dataset_id = "week_4_dbt"
  project    = local.project_name
  location   = local.region
}

