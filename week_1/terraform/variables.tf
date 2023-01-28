variable "BQ_DATASET" {
  description = "Name of the Datawarehouse to create"
  type        = string
  default     = "trips_data_all"
}

variable "TABLE_NAME" {
  description = "Name of the table for our Data Warehouse"
  type        = string
  default     = "my_trips"
}
