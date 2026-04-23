// Pinned provider + Terraform versions. Bump via PR, not in-place.
terraform {
  required_version = ">= 1.7.0, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.62"
    }

    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }

    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }

    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  // TODO_SHELF-27: point at the shelf-bench S3 backend once the results
  // bucket exists. Until then local state is fine for scaffolding.
  // backend "s3" {
  //   bucket = "shelf-bench-tfstate"
  //   key    = "env/terraform.tfstate"
  //   region = "ap-south-1"
  // }
}
