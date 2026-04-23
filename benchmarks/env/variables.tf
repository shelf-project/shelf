// Every knob is here. Terraform fails fast if a default is clearly wrong
// for the caller's region/account.

variable "cluster_name" {
  description = "EKS cluster name. Use a unique suffix per developer so parallel benches do not collide."
  type        = string
  default     = "shelf-bench"
}

variable "region" {
  description = "AWS region. Keep in ap-south-1 for parity with rep-2."
  type        = string
  default     = "ap-south-1"
}

variable "k8s_version" {
  description = "Kubernetes control plane version."
  type        = string
  default     = "1.30"
}

variable "vpc_cidr" {
  type    = string
  default = "10.73.0.0/16"
}

// --------------------------------------------------------------------------
// Node groups. Every instance type is pinned; every count is explicit.
// --------------------------------------------------------------------------

variable "trino_worker_instance_type" {
  description = "Trino worker instance type. Graviton3 preferred once i4g supports NVMe pairing."
  type        = string
  default     = "m6i.2xlarge"
}

variable "trino_worker_count" {
  description = "Initial worker count. cold-start benchmark scales this 2 -> 20."
  type        = number
  default     = 3
}

variable "shelf_node_instance_type" {
  description = "Shelf nodes use NVMe instance storage (i4i or is4gen family)."
  type        = string
  default     = "i4i.2xlarge"
}

variable "shelf_node_count" {
  description = "3-pod StatefulSet (SHELF-21). Never < 3."
  type        = number
  default     = 3
  validation {
    condition     = var.shelf_node_count >= 3
    error_message = "Shelf requires 3 nodes for HRW correctness; see plan §3 Phase 1."
  }
}

variable "driver_instance_type" {
  description = "Benchmark driver pod. Single-node."
  type        = string
  default     = "m6i.large"
}

// --------------------------------------------------------------------------
// Results bucket for published runs. If not given, a bucket named
// <cluster_name>-results is created.
// --------------------------------------------------------------------------

variable "results_bucket" {
  description = "S3 bucket for raw + aggregated results. Empty => create one."
  type        = string
  default     = ""
}

variable "fixture_bucket" {
  description = "S3 bucket with the TPC-DS fixture. See bootstrap.sh for loader."
  type        = string
  default     = ""
}

// --------------------------------------------------------------------------
// Tags applied to every resource for cost attribution.
// --------------------------------------------------------------------------

variable "tags" {
  description = "Common tags. ownership:bench-harness must stay present."
  type        = map(string)
  default = {
    "project"   = "shelf"
    "ownership" = "bench-harness"
    "env"       = "bench"
  }
}
