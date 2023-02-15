terraform {
  required_version = "1.3.6"
  backend "gcs" {
    bucket = "202b9fd304fddf3b-bucket-state"
    prefix = "terraform/state/de_zoomcamp/week_4"
  }
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "=4.50.0"
    }
  }
}

provider "google" {
  project = local.project_name
  region  = local.region
}
