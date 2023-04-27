terraform {
  required_version = "1.3.6"

  backend "local" {}
  # backend "gcs" {
  #   bucket = "202b9fd304fddf3b-bucket-state"
  #   prefix = "terraform/project_7"
  # }
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "=4.50.0"
    }
  }
}

provider "google" {
  project = var.project_name
  region  = var.project_region
}
