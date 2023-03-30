variable "dataset_id" {
  type        = string
  description = "The id of the dataset. Should be unique"
}

variable "project_name" {
  type        = string
  description = "Name of the project to deploy the dataset to"
}

variable "table_ids" {
  type        = list(string)
  description = "list of table names or ids to use for the data warehouse"
}

variable "project_region" {
  type        = string
  description = "region to use to store the data"
}
