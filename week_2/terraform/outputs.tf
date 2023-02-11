output "de_zoomcamp_dataset_id" {
  value       = google_bigquery_dataset.dataset.dataset_id
  description = "The dataset id of the resulting output"
}
