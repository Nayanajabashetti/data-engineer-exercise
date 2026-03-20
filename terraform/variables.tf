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

variable "enable_lambda" {
  description = "Whether to deploy the S3-triggered Lambda path (recommended for small files)."
  type        = bool
  default     = true
}

variable "lambda_memory_size" {
  description = "Lambda memory size in MB."
  type        = number
  default     = 512
}

variable "lambda_timeout_seconds" {
  description = "Lambda timeout in seconds."
  type        = number
  default     = 300
}

variable "ssm_parameter_name" {
  description = "Name of the SecureString parameter stored in SSM Parameter Store."
  type        = string
  default     = "/search-keyword-performance/api-key"
}

variable "enable_db_sinks" {
  description = "Whether Lambda/Glue should also write aggregates to database sinks."
  type        = bool
  default     = false
}

variable "db_host" {
  description = "PostgreSQL host endpoint used by Lambda/Glue DB sink."
  type        = string
  default     = ""
}

variable "db_port" {
  description = "PostgreSQL port used by Lambda/Glue DB sink."
  type        = number
  default     = 5432
}

variable "db_name" {
  description = "PostgreSQL database name for DB sink."
  type        = string
  default     = ""
}

variable "db_secret_arn" {
  description = "Secrets Manager ARN containing PostgreSQL username/password."
  type        = string
  default     = ""
}

variable "db_fact_table" {
  description = "Target PostgreSQL fact table for keyword performance."
  type        = string
  default     = "fact_keyword_performance"
}

variable "db_ai_table" {
  description = "Target PostgreSQL table for keyword AI insights."
  type        = string
  default     = "ai_keyword_insights"
}
