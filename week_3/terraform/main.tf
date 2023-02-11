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

data "terraform_remote_state" "week_2_dataset" {
  backend = "gcs"
  config = {
    bucket = "202b9fd304fddf3b-bucket-state"
    prefix = "terraform/state"
  }
}

resource "google_bigquery_table" "data_warehouse" {
  dataset_id = data.terraform_remote_state.week_2_dataset.outputs.de_zoomcamp_dataset_id
  table_id   = "fhv_data_2019_external"
  project    = local.project_name

  external_data_configuration {
    autodetect    = true
    source_format = "CSV"
    source_uris = [
      "gs://de-zoomcamp-376020_week_3_data_store/fhv/fhv_tripdata_2019-01.csv.gz",
      "gs://de-zoomcamp-376020_week_3_data_store/fhv/fhv_tripdata_2019-02.csv.gz",
      "gs://de-zoomcamp-376020_week_3_data_store/fhv/fhv_tripdata_2019-03.csv.gz",
      "gs://de-zoomcamp-376020_week_3_data_store/fhv/fhv_tripdata_2019-04.csv.gz",
      "gs://de-zoomcamp-376020_week_3_data_store/fhv/fhv_tripdata_2019-05.csv.gz",
      "gs://de-zoomcamp-376020_week_3_data_store/fhv/fhv_tripdata_2019-06.csv.gz",
      "gs://de-zoomcamp-376020_week_3_data_store/fhv/fhv_tripdata_2019-07.csv.gz",
      "gs://de-zoomcamp-376020_week_3_data_store/fhv/fhv_tripdata_2019-08.csv.gz",
      "gs://de-zoomcamp-376020_week_3_data_store/fhv/fhv_tripdata_2019-09.csv.gz",
      "gs://de-zoomcamp-376020_week_3_data_store/fhv/fhv_tripdata_2019-10.csv.gz",
      "gs://de-zoomcamp-376020_week_3_data_store/fhv/fhv_tripdata_2019-11.csv.gz",
      "gs://de-zoomcamp-376020_week_3_data_store/fhv/fhv_tripdata_2019-12.csv.gz"
    ]
    compression = "GZIP"
    csv_options {
      encoding          = "UTF-8"
      skip_leading_rows = 1
      quote             = ""
    }
    schema = <<EOF
[
  {
    "name": "dispatching_base_num",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "pickup_datetime",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },
  {
    "name": "dropOff_datetime",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },
  {
    "name": "PUlocationID",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "DOlocationID",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "SR_Flag",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "Affiliated_base_number",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
EOF

  }

  deletion_protection = false

}
