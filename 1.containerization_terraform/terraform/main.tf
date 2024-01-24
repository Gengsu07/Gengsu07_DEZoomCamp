terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.13.0"
    }
  }
}

provider "google" {
  credentials = "./keys/credentials.json"
  project     = "de-tf-411908"
  region      = "us-central1"
}

resource "google_storage_bucket" "gengsu-bucket" {
  name          = "de-tf-411908_gengsu_bucket"
  location      = "US"
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