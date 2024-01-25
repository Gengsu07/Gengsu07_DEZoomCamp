variable credentials{
    description = "this credentials have been unactivated"
    default = "./keys/credentials.json"
}


variable project{
    description = "Project"
    default = "de-tf-411908"
}

variable region{
    description = "Project Region"
    default = "us-central1"
}


variable "location"{
    description = "My Project Location"
    default="US"
}
variable "bg_dataset_name"{
    description = "Coba buat BigQuery infra"
    default="dataset_latihan"
}
variable "gcs_bucket_name"{
    description = "My Bucket Storage Name"
    default="de-tf-411908_gengsu_bucket"
}

variable "gcs_storage_class" {
    description = "Kelas Bucket Storage GCP"
    default = "STANDARD"
}