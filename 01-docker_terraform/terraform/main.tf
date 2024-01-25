terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.13.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "gengsu-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
  storage_class = var.gcs_storage_class

   lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = var.bg_dataset_name
  friendly_name               = "bigquery-dataset_test"
  description                 = "This is a test description"
  location                    = var.location

}