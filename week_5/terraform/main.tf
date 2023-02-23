resource "google_storage_bucket" "spark_gcs" {
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
