variable "BQ_DATASET" {
  description = "Name of the Datawarehouse to create"
  type        = string
  default     = "de_zoomcamp_wh"
}

variable "TABLE_NAME" {
  description = "Name of the table for our Data Warehouse"
  type        = string
  default     = "my_trips"
}
