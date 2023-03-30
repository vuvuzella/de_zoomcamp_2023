output "dataset_id" {
  value       = google_bigquery_dataset.dataset.dataset_id
  description = "dataset id for the data warehouse"
}

output "tables_list" {
  value       = google_bigquery_table.table[*].table_id
  description = "List of tables in the data warehouse"
}
