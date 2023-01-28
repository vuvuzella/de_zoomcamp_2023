terraform {
  required_version = "1.3.6"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "=4.50.0"
    }
  }
}

provider "google" {
  project = "de-zoomcamp-376020" # project id of where the resources will belong to
  region  = "australia-southeast1"
}

resource "random_id" "bucket_prefix" {
  byte_length = 8
}

resource "google_storage_bucket" "default" {
  name          = "${random_id.bucket_prefix.hex}-bucket-state"
  force_destroy = false
  location      = "australia-southeast1"
  storage_class = "STANDARD"
  versioning {
    enabled = true
  }
}
