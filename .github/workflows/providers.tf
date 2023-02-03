provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

terraform {
  backend "gcs" {
    bucket = "bucket-dataproject2"
    prefix = "terraform/state"
  }
}