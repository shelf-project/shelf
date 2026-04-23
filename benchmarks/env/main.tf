// EKS cluster skeleton for the Shelf benchmark harness.
// TODO_SHELF-27: this is the scaffolding. The real module wiring lands
// when `make env-up` is first green; for v0.0 we declare intent only.

provider "aws" {
  region = var.region

  default_tags {
    tags = var.tags
  }
}

// --------------------------------------------------------------------------
// Networking
// --------------------------------------------------------------------------

module "vpc" {
  // TODO_SHELF-27: pin the module version once a first run is green.
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.8"

  name = var.cluster_name
  cidr = var.vpc_cidr
  azs  = ["${var.region}a", "${var.region}b", "${var.region}c"]

  private_subnets = ["10.73.1.0/24", "10.73.2.0/24", "10.73.3.0/24"]
  public_subnets  = ["10.73.101.0/24", "10.73.102.0/24", "10.73.103.0/24"]

  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true

  tags = var.tags
}

// --------------------------------------------------------------------------
// EKS cluster + node groups
// --------------------------------------------------------------------------

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.16"

  cluster_name    = var.cluster_name
  cluster_version = var.k8s_version

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_endpoint_public_access = true

  // Three pinned node groups. Spot only allowed on the fixture loader.
  eks_managed_node_groups = {
    trino = {
      instance_types = [var.trino_worker_instance_type]
      min_size       = 2
      max_size       = 20
      desired_size   = var.trino_worker_count
      labels         = { role = "trino" }
      taints = {
        trino = { key = "role", value = "trino", effect = "NO_SCHEDULE" }
      }
    }

    shelf = {
      instance_types = [var.shelf_node_instance_type]
      min_size       = var.shelf_node_count
      max_size       = var.shelf_node_count
      desired_size   = var.shelf_node_count
      labels         = { role = "shelf" }
      taints = {
        shelf = { key = "role", value = "shelf", effect = "NO_SCHEDULE" }
      }
      // NVMe is instance-local, no extra EBS.
    }

    driver = {
      instance_types = [var.driver_instance_type]
      min_size       = 1
      max_size       = 1
      desired_size   = 1
      labels         = { role = "driver" }
    }
  }

  tags = var.tags
}

// --------------------------------------------------------------------------
// Results bucket. Created only if the caller did not supply one.
// --------------------------------------------------------------------------

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

locals {
  results_bucket_name = var.results_bucket != "" ? var.results_bucket : "${var.cluster_name}-results-${random_id.bucket_suffix.hex}"
}

resource "aws_s3_bucket" "results" {
  count  = var.results_bucket == "" ? 1 : 0
  bucket = local.results_bucket_name
  tags   = var.tags
}

// TODO_SHELF-26: lifecycle policy (90 d hot / 1 yr warm / forever cold)
// TODO_SHELF-26: bucket policy for public-read mirror prefix
// TODO_SHELF-27: CloudWatch metric subscription for Grafana live view
