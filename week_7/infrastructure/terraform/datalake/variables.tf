variable "datalake_name" {
  type        = string
  default     = "fp-data-lake"
  description = "Google Cloud Storage for storing data needed to be loaded to datawarehouses"
}

variable "dataset_name" {
  type        = string
  description = "Name for the dataset to create in BigQuery"
}

variable "project_name" {
  type        = string
  description = "Name of the project to deploy these resources to"
}

variable "project_region" {
  type        = string
  default     = "australia-southeast1"
  description = "Google region to deploy resources to"
}
