module "datalake" {
  source = "./datalake"

  project_name   = var.project_name
  project_region = var.project_region
  dataset_name   = "australian_housing_875628"
}

module "data_warehouse" {
  source         = "./data_warehouse"
  dataset_id     = "anz_road_crash_dataset"
  project_name   = var.project_name
  project_region = var.project_region
  table_ids = [
    "raw_casualties",
    "raw_crashes",
    "raw_datetimes",
    "raw_descriptions",
    "raw_locations",
    "raw_vehicles"
  ]
}
