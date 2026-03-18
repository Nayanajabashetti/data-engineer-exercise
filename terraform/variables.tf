variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-west-2"
}

variable "bucket_name" {
  description = "Name of the S3 bucket for input and output data."
  type        = string
}

variable "input_prefix" {
  description = "S3 key prefix for input hit-level data files."
  type        = string
  default     = "input/"
}

variable "output_prefix" {
  description = "S3 key prefix where results are written."
  type        = string
  default     = "output/"
}

variable "glue_worker_type" {
  description = "Glue worker type (G.1X = 4 vCPU/16GB, G.2X = 8 vCPU/32GB)."
  type        = string
  default     = "G.1X"
}

variable "glue_worker_count" {
  description = "Number of Glue workers. Scale up for larger files."
  type        = number
  default     = 2
}
