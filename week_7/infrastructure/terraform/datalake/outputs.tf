output "datalake_name" {
  value = google_storage_bucket.data-lake-bucket.name
}

output "datalake_id" {
  value = google_storage_bucket.data-lake-bucket.id
}
