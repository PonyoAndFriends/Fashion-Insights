# vpc 생성

variable "project_id" {
  description = "fasion-insights"
}

variable "region" {
  description = "asia-northeast3"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# VPC
resource "google_compute_network" "vpc" {
  name                    = "${var.project_id}-vpc"
  auto_create_subnetworks = "false"
}

# Subnet
resource "google_compute_subnetwork" "subnet" {
  name          = "${var.project_id}-subnet"
  region        = var.region
  network       = google_compute_network.vpc.name
  ip_cidr_range = "10.10.0.0/16"

  private_ip_google_access = true  # VPC 내부에서 GCP API 접근 가능, VPC CNI를 위함.

  # VPC CNI를 위한 Secondary Range 설정
  secondary_ip_range {
    range_name    = "pods-range"
    ip_cidr_range = "10.20.0.0/16"
  }

  secondary_ip_range {
    range_name    = "services-range"
    ip_cidr_range = "10.30.0.0/16"
  }
}

