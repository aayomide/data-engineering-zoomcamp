variable "credentials" {
  description = "Credentials"
  default     = "C:/Users/pc/Documents/projects/data_engineering/week_1_basics_n_setup/terraform_gcp/.keys/de-zmcmp-a5e5a0047f0b.json"  #"<Path to your Service Account json file>"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "Name of Project"
  default     = "de-zmcmp"
  type        = string
}

variable "region" {
  description = "Regional location of GCP resources. Choose based on your location: https://cloud.google.com/about/locations"
  default     = "europe-west9"
  type        = string
}

variable "location" {
  description = "Project Location"
  default     = "EU"
  type        = string
}

variable "gcp_storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "GCS Bucket Name"
  default     = "de-zmcmp-bucket-150124"
  type        = string
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  default     = "zmcmp_dataset_150124"
  type        = string
}