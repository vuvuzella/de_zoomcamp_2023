locals {
  project_name = "de-zoomcamp-376020"
  region       = "australia-southeast1"

  bucket_name = ""
}

resource "google_storage_bucket" "data-lake-bucket" {
  name     = "${local.project_name}_week_2_data_store"
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
