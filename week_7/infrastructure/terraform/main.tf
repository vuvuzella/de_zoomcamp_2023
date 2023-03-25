module "datalake" {
  source = "./datalake"

  project_name   = var.project_name
  project_region = var.project_region
  dataset_name   = "australian_housing_875628"
}
