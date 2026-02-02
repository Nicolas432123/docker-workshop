variable "project" {
  description = "project variable"
  default     = "terraform-demo-485213"
}

variable "region" {
  description = "region"
  default     = "us-central1"
}

variable "credentials" {
  description = "credentials"
  default     = "./keys/my-creds.json"
}


variable "location" {
  description = "My BigQuery Dataset Name"
  default     = "AUSTRALIA-SOUTHEAST1"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "demo_dataset"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
