terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  # Configuration options
  # cpredentials = "/keys/my-creds.json"
  credentials = file(var.credentials)
  project = var.project
  region  = "us-central1"
}


resource "google_storage_bucket" "project-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "project-dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}