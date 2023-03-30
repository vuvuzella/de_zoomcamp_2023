
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.dataset_id
  location   = "australia-southeast1"
}

resource "google_bigquery_table" "table" {
  count = length(var.table_ids)

  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = var.table_ids[count.index]

  # TODO: create partitioning and clustering for the tables
}
